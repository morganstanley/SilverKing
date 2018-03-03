// Util.c

/////////////
// includes

#include <assert.h>
#include <ctype.h>
#include <execinfo.h>
#include <fcntl.h>
#include <fuse.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <zlib.h>

#include <cxxabi.h>
#include <execinfo.h>
#include <ucontext.h>

#include "skconstants.h"
#include "QueueProcessor.h"
#include "SRFSDHT.h"
#include "SRFSConstants.h"
#include "Util.h"


////////////////////
// private defines

#define LOG_MAX_ENTRY_LENGTH 16384
#define LOG_MAX_FORMAT_LENGTH 16384
//#define LogFile stderr
#define _BT_LIST_SIZE	16384
#define	_PATH_CHAR '/'
#define _PATH_CUR_DIR "."
#define _PATH_PARENT_DIR ".."
#define LOG_QUEUE_SIZE	16
#define LOG_MAX_BATCH_SIZE	16
#define LOG_THREADS 1


#define _FLUSH_LOG
#define ASYNC_LOGGING

// Below is for debugging memory allocation/deallocation
// Normally this is commented out
//#define DEBUG_MEMORY

////////////////////
// private members

static LogLevel	currentLogLevel = LOG_WARNING;
static int _fatalErrorWarnOnly;
static uint64_t	fatalErrorCount;
static FILE	*LogFile = stderr;
static QueueProcessor	*logQP;


///////////////////////
// private prototypes

static void cv_wait_abs_given_time(pthread_mutex_t *mutex, pthread_cond_t *cv, uint64_t deadline, uint64_t currentTime);
static void log_process_batch(void **logEntries, int numLogEntries, int curThreadIndex);


///////////////
// globals

char zeroBlock[SRFS_BLOCK_SIZE];
uint64_t    myValueCreator;


///////////////////
// implementation

// logging

/**
 * create new format string for single call to printf function(s)
 */
static inline void modifyformat( char *newfmt, char const* format, char const* errfmt) {
	struct timespec ts;
	struct tm now;
	
	clock_gettime(CLOCK_REALTIME, &ts);
	if (localtime_r(&ts.tv_sec, &now)) {
		long us = ts.tv_nsec / 1000;
		sprintf(newfmt, "%02u %02u:%02u:%02u.%06lu %4x %s%s\n",
					  now.tm_mday,
					  now.tm_hour,
					  now.tm_min,
					  now.tm_sec,
					  us,
#ifdef _WIN32
					  (int)pthread_self().p,
#else
					  (int)pthread_self() & 0xffff,
#endif
					  format,
					  errfmt
					  );
		/*
		sprintf(newfmt, "[%u-%02u-%02u %02u:%02u:%02u.%06lu][0x%x] %s%s\n",
					  now.tm_year + 1900,
					  now.tm_mon + 1,
					  now.tm_mday,
					  now.tm_hour,
					  now.tm_min,
					  now.tm_sec,
					  ms,
#ifdef _WIN32
					  (int)pthread_self().p,
#else
					  (int)pthread_self(),
#endif
					  format,
					  errfmt
					  );
	*/
	}
}

void srfsLogFlush() {
	fflush(LogFile);
}

void srfsLogSetFile(char *fileName) {
	int logFileNameLen = strlen(fileName);
	if ( logFileNameLen > 0 && logFileNameLen < SRFS_MAX_PATH_LENGTH ) 
	{
		if( LogFile && LogFile != stderr) {
			fclose(LogFile);
			LogFile = NULL;
		}

	    LogFile = fopen(fileName, "w");
		if ( LogFile ) {
			srfsLog(LOG_WARNING, "Opened log file %s", fileName);
			return ;
		}
	}

	LogFile = stderr;
	srfsLog(LOG_WARNING, "Failed to open log file %s. Using stderr", fileName);
}

void srfsRedirectStdio() {
    if (dup2(fileno(LogFile), fileno(stdout)) != fileno(stdout)) {
        fatalError("failed to redirect stdout", __FILE__, __LINE__);
    }
    if (dup2(fileno(LogFile), fileno(stderr)) != fileno(stderr)) {
        fatalError("failed to redirect stderr", __FILE__, __LINE__);
    }
    fprintf(stdout, "stdout\n");
    fprintf(stderr, "stderr\n");
    fflush(0);
}

void srfsLogInitAsync() {
#ifdef ASYNC_LOGGING
	logQP = qp_new_batch_processor(log_process_batch, __FILE__, __LINE__, 
								LOG_QUEUE_SIZE, ABQ_FULL_BLOCK, LOG_THREADS, LOG_MAX_BATCH_SIZE);
	srfsLog(LOG_WARNING, "logQP %llx %llx", logQP, logQP->abq);
#endif
}

void srfsLogAsync(LogLevel level, char const * format, ...) {
	if (level <= currentLogLevel) {
		char newfmt[LOG_MAX_FORMAT_LENGTH];
		va_list ap;
		
		va_start(ap, format);
		// this is for a single MT-safe call to vfprintf with everything
		modifyformat(newfmt, format, "");
#ifdef ASYNC_LOGGING
		{
            char tmpbuf[LOG_MAX_ENTRY_LENGTH];
			char *buf;
			
			vsprintf(tmpbuf, newfmt, ap);
            buf = strdup(tmpbuf);
			//srfsLog(LOG_FINE, "srfsLogAsync qp_add %llx", buf);
			qp_add(logQP, buf);
		}
#else
		vfprintf(LogFile, newfmt, ap);
#ifdef _FLUSH_LOG
		fflush(LogFile);
#endif
#endif
		va_end(ap);
	}
}

void srfsLog(LogLevel level, char const * format, ...) {
	if (level <= currentLogLevel) {
		char newfmt[LOG_MAX_FORMAT_LENGTH];
		va_list ap;
		
		va_start(ap, format);
		// this is for a single MT-safe call to vfprintf with everything
		modifyformat(newfmt, format, "");
		vfprintf(LogFile, newfmt, ap);
#ifdef _FLUSH_LOG
		fflush(LogFile);
#endif
		va_end(ap);
	}
}

static void log_process_batch(void **logEntries, int numLogEntries, int curThreadIndex) {
	int	i;
	
	//srfsLog(LOG_FINE, "log_process_batch %llx %d", logQP, numLogEntries);
	for (i = 0; i < numLogEntries; i++) {
		char	*buf;
		
		buf = (char *)(logEntries[i]);
		fprintf(LogFile, "%s", buf);
		free(buf);
	}
	//srfsLog(LOG_FINE, "out log_process_batch %d", numLogEntries);
}

void setSRFSLogLevel(LogLevel level) {
	currentLogLevel = level;
    LoggingLevel llvl = level==LOG_ERROR ? LVL_OFF : level==LOG_FINE ? LVL_ALL : LVL_INFO ;
}

int srfsLogLevelMet(LogLevel level) {
	return currentLogLevel >= level;
}

// time

uint64_t curTimeMillis() {
    struct timeval	tv;
	uint64_t	rVal;

	gettimeofday(&tv, NULL);
	rVal = (uint64_t)tv.tv_sec * (uint64_t)1000 + (uint64_t)tv.tv_usec / (uint64_t)1000;
	return rVal;
}

uint64_t curTimeMicros() {
    struct timeval	tv;
	uint64_t	rVal;

	gettimeofday(&tv, NULL);
	rVal = (uint64_t)tv.tv_sec * (uint64_t)1000000 + (uint64_t)tv.tv_usec;
	return rVal;
}

// error handling

void setFatalErrorWarnOnly(int warnOnly) {
	_fatalErrorWarnOnly = warnOnly;
}

void resetFatalErrorCount() {
	fatalErrorCount = 0;
}

uint64_t getFatalErrorCount() {
	return fatalErrorCount;
}

void fatalError(char *msg, char *file, int line) {
	void	*arr[_BT_LIST_SIZE];
	size_t	size;
	char	**strings;
	size_t	i;

	fatalErrorCount++;
	if (line != 0) {
		srfsLog(LOG_ERROR, "Fatal Error: %s %d: %s", file, line, msg);
	} else {
		srfsLog(LOG_ERROR, "Fatal Error: %s", msg);
	}

	print_stacktrace("fatalError", stderr, _BT_LIST_SIZE );
	srfsLogFlush();
	fflush(0);
	if (!_fatalErrorWarnOnly) {
		raise(SIGSEGV);
		exit(-1);
	} else {
		srfsLog(LOG_ERROR, "Fatal error is set to warn only");
	}
}

void dumpCoreAndContinue() {
    if (!fork()) {
        abort();
    }
}

void *_mem__alloc(size_t nmemb, size_t size, char *file, int line) {
	void	*ptr;

	ptr = calloc(nmemb, size);
#ifdef DEBUG_MEMORY
    if (file != NULL) {
        srfsLog(LOG_WARNING, "mem__alloc %llx %u %s %d", ptr, nmemb * size, file, line);
    }
#endif
	if (ptr == NULL) {
		fatalError("out of memory", file, line);
	}
	return ptr;
}

void _mem__free(void **ptr, char *file, int line) {
	if (ptr == NULL) {
		fatalError("mem__free called with NULL ptr", file, line);
	}
	if (*ptr == NULL) {
		fatalError("mem__free called with NULL *ptr", file, line);
	}
#ifdef DEBUG_MEMORY
	srfsLog(LOG_WARNING, "mem__free %llx %s %d", *ptr, file, line);
#endif
	free(*ptr);
	*ptr = NULL;
}

void _mem__realloc(void **ptr, size_t o_nmemb, size_t nmemb, size_t size, char *file, int line) {
	void	*_ptr;
	
	if (ptr == NULL) {
		fatalError("mem__realloc called with NULL ptr", file, line);
	}
	if (nmemb <= o_nmemb) {
		fatalError("mem__realloc called with nmemb <= o_nmemb", file, line);
	}
	_ptr = realloc(*ptr, nmemb * size);
	if (_ptr == NULL) {
		fatalError("out of memory", file, line);
	}
	memset((void *)((const char *)_ptr + o_nmemb * size), 0, (nmemb - o_nmemb) * size);
#ifdef DEBUG_MEMORY
	srfsLog(LOG_WARNING, "mem__realloc %llx %llx %u %s %d", *ptr, _ptr, nmemb * size, file, line);
#endif
	*ptr = _ptr;
}

void *_mem__dup(const void *source, int size, char *file, int line) {
	void	*dest;

	dest = _mem__alloc(size, 1, NULL, 0);
	memcpy(dest, source, size);
#ifdef DEBUG_MEMORY
	srfsLog(LOG_WARNING, "mem__dup %llx %llx %u %s %d", source, dest, size, file, line);
#endif
	return dest;
}

char *_str__dup(const char *source, char *file, int line) {
	return (char *)_mem__dup(source, strlen(source) + 1, file, line);
}

int *_int__dup(int *i, char *file, int line) {
	return (int *)_mem__dup(i, sizeof(int), file, line);
}

int strcntc(char *s, char c) {
	char	*p;
	int		numC;

	numC = 0;
	p = s;
	while (*p) {
		if (*p == c) {
			++numC;
		}
		++p;
	}
	return numC;
}

char **str_alloc_array(int r, size_t size) {
    char    **a;
    int     i;
    
    a = (char **)mem_alloc(r, sizeof(char *));
    for (i = 0; i < r; i++) {
        a[i] = (char *)mem_alloc(1, size);
    }
    return a;
}

void str_free_array(char ***a, int r) {
    int i;
    
    for (i = 0; i < r; i++) {
        mem_free((void **)&(*a)[i]);
    }
    mem_free((void **)a);
}

// pthread helpers

void mutex_init(pthread_mutex_t *mutex, pthread_mutex_t **mutexPtr) {
    int rc;
    
	rc = pthread_mutex_init(mutex, NULL);
    if (rc != 0) {
        fatalError("pthread_mutex_init() failed", __FILE__, __LINE__);
    }
	*mutexPtr = mutex;
}

void cv_init(pthread_cond_t *cv, pthread_cond_t **cvPtr) {
    int rc;
    
	rc = pthread_cond_init(cv, NULL);
    if (rc != 0) {
        fatalError("pthread_cond_init() failed", __FILE__, __LINE__);
    }
	*cvPtr = cv;
}

void mutex_destroy(pthread_mutex_t **mutexPtr) {
	if (mutexPtr == NULL) {
		fatalError("NULL mutexPtr", __FILE__, __LINE__);
	}
	if (*mutexPtr == NULL) {
		fatalError("NULL *mutexPtr", __FILE__, __LINE__);
	}
	pthread_mutex_destroy(*mutexPtr);
	*mutexPtr = NULL;
}

void cv_destroy(pthread_cond_t **cvPtr) {
	if (cvPtr == NULL) {
		fatalError("NULL cvPtr", __FILE__, __LINE__);
	}
	if (*cvPtr == NULL) {
		fatalError("NULL *cvPtr", __FILE__, __LINE__);
	}
	pthread_cond_destroy(*cvPtr);
	*cvPtr = NULL;
}

void cv_wait_rel(pthread_mutex_t *mutex, pthread_cond_t *cv, uint64_t interval) {
	cv_wait_abs(mutex, cv, curTimeMillis() + interval);
}

void cv_wait_abs(pthread_mutex_t *mutex, pthread_cond_t *cv, uint64_t deadline) {
	cv_wait_abs_given_time(mutex, cv, deadline, curTimeMillis());
}

static void cv_wait_abs_given_time(pthread_mutex_t *mutex, pthread_cond_t *cv, uint64_t deadline, uint64_t currentTime) {
	srfsLog(LOG_FINE, "cv_wait_abs %llu %llu", deadline, currentTime);
	if (deadline > currentTime) {
		struct timespec	ts;

		memset(&ts, 0, sizeof(struct timespec));
		ts.tv_sec = deadline / 1000;
		ts.tv_nsec = (deadline % 1000) * 1000000;
		srfsLog(LOG_FINE, "pthread_cond_timedwait %d %d", ts.tv_sec, ts.tv_nsec);
		pthread_cond_timedwait(cv, mutex, &ts);
	}
	srfsLog(LOG_FINE, "out cv_wait_abs");
}

// string utilities

unsigned int stringHash(void *s) {
    unsigned char *str;
    unsigned long hash;
    int c;
   
    str = (unsigned char *)s;
    hash = 0;
    while ((c = *str++)!=0) {
		hash = c + (hash << 6) + (hash << 16) - hash;
    }
    //printf("hashString %s %d %x\n", s, strlen((char *)s), hash);
    return hash;
}

int suffixMatches(const char *s, const char *suffix) {
	int	sLength;
	int	suffixLength;

	sLength = strlen(s);
	suffixLength = strlen(suffix);
	if (suffixLength > sLength) {
		return FALSE;
	} else {
		return !strcmp(&s[sLength - suffixLength], suffix);
	}
}

int string_count_occurrences(char *s, int c) {
	char	*s1;
	int		count;

	count = 0;
	s1 = s;
	while (s1 != NULL) {
		s1 = strchr(s1, c);
		if (s1 != NULL) {
			count++;
			s1++;
		}
	}
	return count;
}

void ensure_null_terminated(char *s, char *limit, char *f, int l) {
	char	*c;
	
	for (c = s; c < limit; c++) {
		if (*c == '\0') {
			return;
		}
	}
	fatalError("Null terminator not found", f, l);
}

// path

int is_base_path(char *path) {
    if (path[0] != '\0' && path[1] != '\0') {
        int depth;
        
        depth = string_count_occurrences(path, _PATH_CHAR);
        return depth == 1;
    } else {
        return FALSE;
    }
}

// could consider in-place strok() based version
// for now, no

void path_display_elements(char **pathElements, int numElements) {
	int	i;

	for (i = 0; i < numElements; i++) {
		printf("%d\t%s\n", i, pathElements[i]); fflush(0);
	}
}

char **path_split_elements(char *path, int *numElements) {
	char	tmp[SRFS_MAX_PATH_LENGTH];
	int		tmpLength;

	char	**pathElements;
	char	*lastElement;
	char	*nextElement;
	int		nElements;
	int		i;
	char	*_path;
	
	memset(tmp, 0, SRFS_MAX_PATH_LENGTH);
	strcpy(tmp, path);

	// ensure that we have a trailing _PATH_CHAR
	tmpLength = strlen(tmp);
	if (tmpLength == 0) {
		tmp[0] = _PATH_CHAR;
	} else if (tmpLength == 1) {
		tmp[1] = _PATH_CHAR;
	} else if (tmp[tmpLength - 1] != _PATH_CHAR) {
		tmp[tmpLength] = _PATH_CHAR;
	}
	// correct nElements for possible leading _PATH_CHAR
	nElements = string_count_occurrences(tmp, _PATH_CHAR);
	if (*tmp == _PATH_CHAR) {
		nElements--;
	} else {
	}

	_path = tmp;
	lastElement = tmp;
	//srfsLog(LOG_WARNING, "nElements %d", nElements);
	pathElements = (char **)mem_alloc(nElements, sizeof(char *));
	for (i = 0; i < nElements; i++) {
		int	elementSize;

		nextElement = strchr(lastElement + 1, _PATH_CHAR);
		elementSize = nextElement - lastElement;
		//srfsLog(LOG_WARNING, ":: %d %d %llx %llx", i, elementSize, lastElement, nextElement);
		pathElements[i] = (char *)mem_alloc(elementSize + 1, 1);
		memcpy(pathElements[i], lastElement, elementSize);
		lastElement = nextElement;
	}
	*numElements = nElements;
	return pathElements;
}

int path_merge_elements(char **elements, int numElements, char *buf, int bufSize) {
	char	tmp[SRFS_MAX_PATH_LENGTH];
	int		count;
	int		i;
	char	*dest;

	count = 0;
	dest = tmp;
	for (i = 0; i < numElements; i++) {
		if (elements[i] != NULL) {
			int	len;

			len = strlen(elements[i]);
			memcpy(dest, elements[i], len);
			dest += len;
			//if (i < numElements - 1) {
			//	*dest = _PATH_CHAR;
			//}
			//dest++;
		}
	}
	count = dest - tmp;
	if (count > bufSize) {
		count = bufSize;
	}
	memset(buf, 0, bufSize);
	memcpy(buf, tmp, count);
	return count;
}

void path_free_elements(char ***pathElements, int numElements) {
	if (pathElements != NULL && *pathElements != NULL) {
		int	i;

		for (i = 0; i < numElements; i++) {
			if ((*pathElements)[i] != NULL) {
				mem_free((void **)&(*pathElements)[i], __FILE__, __LINE__);
			}
		}
		mem_free((void **)pathElements, __FILE__, __LINE__);
	}
}

static void path_shift_elements(char **elements, int numElements, int sourceIndex, int destIndex) {
	int	i;
	int	numToShift;
	int	firstNonShifted;

	if (destIndex >= sourceIndex) {
		srfsLog(LOG_ERROR, "bad indices %d %d", sourceIndex, destIndex);
		fatalError("bad indices", __FILE__, __LINE__);
	}
	numToShift = numElements - sourceIndex;
	if (numToShift < 0) {
		srfsLog(LOG_ERROR, "bad numToShift %d", numToShift);
		fatalError("bad numToShift", __FILE__, __LINE__);
	}
	for (i = 0; i < numToShift; i++) {
		if (elements[destIndex + i] != NULL) {
			mem_free((void **)&elements[destIndex + i], __FILE__, __LINE__);
		} 
		elements[destIndex + i] = elements[sourceIndex + i];
		elements[sourceIndex + i] = NULL;
	}
	firstNonShifted = destIndex + numToShift;
	for (i = firstNonShifted; i < numElements - numToShift ; i++) {
		mem_free((void **)&elements[i], __FILE__, __LINE__);
	}
}

static int path_element_matches(char *element, char *match) {
	if (element == NULL) {
		return FALSE;
	} else {
		if (*element == _PATH_CHAR) {
			element++;
		}
		return !strcmp(element, match);
	}
}

int path_simplify(char *path, char *buf, int bufSize) {
	char	**pathElements;
	int		numElements;
	int		curIndex;
	int		lastIndex;

	pathElements = path_split_elements(path, &numElements);
	//path_display_elements(pathElements, numElements);

	curIndex = 0;
	lastIndex = numElements - 1;
	while (curIndex <= lastIndex) { 
		//path_display_elements(pathElements, numElements);
		//srfsLog(LOG_WARNING, "%d %d", curIndex, lastIndex);
		if (path_element_matches(pathElements[curIndex], _PATH_CUR_DIR)) {
			if (curIndex < lastIndex) {
				path_shift_elements(pathElements, numElements, curIndex + 1, curIndex);
			} else {
				mem_free((void **)&pathElements[curIndex], __FILE__, __LINE__);
			}
			lastIndex--;
		} else if (path_element_matches(pathElements[curIndex], _PATH_PARENT_DIR)) {
			if (curIndex > 0) {
				if (curIndex < lastIndex) {
					path_shift_elements(pathElements, numElements, curIndex + 1, curIndex - 1);
					lastIndex -= 2;
				} else {
					mem_free((void **)&pathElements[curIndex], __FILE__, __LINE__);
					mem_free((void **)&pathElements[curIndex - 1], __FILE__, __LINE__);
				}
				curIndex--;
			} else {
				path_free_elements(&pathElements, numElements);
				return FALSE;
			}
		} else {
			curIndex++;
		}
	}

	//path_display_elements(pathElements, numElements);

	path_merge_elements(pathElements, numElements, buf, bufSize);

	path_free_elements(&pathElements, numElements);

	return TRUE;
}

char *path_prev(char *c) {
	if (*c != _PATH_CHAR) {
		fatalError("not path separator", __FILE__, __LINE__);
	} else {
		--c;
		while (*c != _PATH_CHAR) {
			--c;
		}
		return c;
	}
	return NULL;
}


// file

char *file_read(char *path, size_t *length) {
	if (path == NULL) {
		return NULL;
	} else {
		FILE	*f;
		size_t	size;
		char	*buf;
		int	rVal;
		size_t	numRead;
		
		f = fopen(path, "rb");
		if (f == NULL) {
			return NULL;
		} 
		rVal = fseek(f, 0, SEEK_END);
		if (rVal < 0) {
			return NULL;
		}
		size = ftell(f);
		if (rVal < 0) {
			return NULL;
		}
		rVal = fseek(f, 0, SEEK_SET);
		if (rVal < 0) {
			return NULL;
		}
        // allocates size + 1 so that the last byte will act as a string terminator
		buf = (char *)mem_alloc(size + 1, 1, __FILE__, __LINE__);
		numRead = fread(buf, 1, size, f);
		if (numRead != size) {
			return NULL;
		}
		fclose(f);
        if (length != NULL) {
            *length = size;
        }
		return buf;
	}
}

int file_read_partial(const char *path, char *dest, size_t readSize, off_t readOffset) {
	if (path == NULL) {
		return -1;
	} else {
		int	fd;
		
		size_t	size;
		char	*buf;
		int	rVal;
		size_t	totalRead;
		
		fd = open(path, O_RDONLY);
		if (fd < 0) {
			return -1;
		}		
		rVal = lseek(fd, readOffset, SEEK_SET);
		if (rVal < 0) {
			return -1;
		}
		totalRead = 0;
		while (totalRead < readSize) {
			long  numRead;
			
			numRead = read(fd, dest + totalRead, readSize - totalRead);
			if (numRead < 0) {
				return -1;
			} else {
				if (totalRead > 0) {
					totalRead += numRead;
				} else {
					totalRead = readSize;
				}
			}
		}
		rVal = close(fd);
		if (rVal < 0) {
			srfsLog(LOG_ERROR, "Ignoring error in closing %s\n", path);
		}
		return totalRead;
	}
}

size_t trim_in_place(char *s, size_t length) {
    int i;
    
    for (i = length - 1; i >= 0; i--) {
        if (isspace(s[i])) {
            s[i] = '\0';
        } else {
            return (size_t)(i + 1);
        }
    }
    return 0;
}

// stat

void stat_display(struct stat *s, FILE *f) {
	fprintf(f, "st_dev    \t%x\n", (unsigned int) s->st_dev);
	fprintf(f, "st_ino    \t%lu\n", s->st_ino);
	fprintf(f, "st_mode   \t%x\n", s->st_mode);
	fprintf(f, "st_nlink  \t%d\n", (int) s->st_nlink);
	fprintf(f, "st_uid    \t%d\n", s->st_uid);
	fprintf(f, "st_gid    \t%d\n", s->st_gid);
	fprintf(f, "st_rdev   \t%x\n", (unsigned int) s->st_rdev);
	fprintf(f, "st_size   \t%ld\n", s->st_size);
	fprintf(f, "st_atime  \t%ld\n", s->st_atime);
	fprintf(f, "st_mtime  \t%ld\n", s->st_mtime);
	fprintf(f, "st_ctime  \t%ld\n", s->st_ctime);
	fprintf(f, "st_blksize\t%ld\n", s->st_blksize);
	fprintf(f, "st_blocks \t%ld\n", s->st_blocks);
	//fprintf(f, "st_attr%x\n", s->st_attr);
}

uint64_t stat_mtime_micros(struct stat *s) {
    return s->st_mtime * 1000000 + (s->st_mtim.tv_nsec / 1000);
}

uint64_t stat_mtime_millis(struct stat *s) {
    return s->st_mtime * 1000 + (s->st_mtim.tv_nsec / 1000000);
}

// misc

unsigned int mem_hash(void *m, int size) {
    unsigned char *str;
    unsigned long hash;
	int	i;
    int c;
   
    str = (unsigned char *)m;
    hash = 0;
	for (i = 0; i < size; i++) {
	    c = *str;
		str++;
		hash = c + (hash << 6) + (hash << 16) - hash;
    }
    return hash;
}

size_t size_max(size_t a, size_t b) {
	return a >= b ? a : b;
}

size_t size_min(size_t a, size_t b) {
	return a <= b ? a : b;
}

int int_max(int a, int b) {
	return a >= b ? a : b;
}

int int_min(int a, int b) {
	return a <= b ? a : b;
}

off_t off_max(off_t a, off_t b) {
	return a >= b ? a : b;
}

off_t off_min(off_t a, off_t b) {
	return a <= b ? a : b;
}

uint64_t uint64_max(off_t a, off_t b) {
	return a >= b ? a : b;
}

uint64_t uint64_min(off_t a, off_t b) {
	return a <= b ? a : b;
}

int get_num_cpus() {
	return sysconf(_SC_NPROCESSORS_ONLN);
}

int get_pid() {
	return (int)getpid();
}

uint64_t getValueCreatorAsUint64(SKValueCreator *vc) {
    SKVal       *pVal;
    uint64_t    v;
    
    pVal = vc->getBytes();
    if (pVal == NULL) {
        v = 0;
    } else {
        if (pVal->m_pVal == NULL) {
            v = 0;
        } else {
            v = *((uint64_t *)pVal->m_pVal);
        }
        sk_destroy_val(&pVal);
    }
    return v;
}

int zlibBuffToBuffDecompress(char *dest, int *destLength, 
						   char* source, int sourceLength) {
    int ret;
    z_stream strm;
	int	originalDestLength;

	originalDestLength = *destLength;

    /* allocate inflate state */
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    ret = inflateInit(&strm);
	if (ret != Z_OK) {
        return ret;
	}

    strm.avail_in = sourceLength;
    strm.next_in = (Bytef*)source;
    strm.avail_out = *destLength;
    strm.next_out = (Bytef*)dest;

    ret = inflate(&strm, Z_FINISH);
    assert(ret != Z_STREAM_ERROR);  /* state not clobbered */

	if (ret == Z_STREAM_END) {
		*destLength = originalDestLength - strm.avail_out;
	}

    /* clean up and return */
    (void)inflateEnd(&strm);
	if (ret != Z_STREAM_END) {
		srfsLog(LOG_ERROR, "ret != Z_STREAM_END\t%d", ret);
		srfsLog(LOG_ERROR, "strm.msg: %s", strm.msg);
	}
    return ret == Z_STREAM_END ? Z_OK : Z_DATA_ERROR;
}

time_t epoch_time_seconds() {
	return time(NULL);
}

void sleep_random_millis(uint64_t minMillis, uint64_t maxMillis, unsigned int *seedp) {
	usleep( (rand_r(seedp) % (maxMillis - minMillis + 1) + minMillis) * 1000 );
}

uid_t get_uid() {
	struct fuse_context	*context;
	
	context = fuse_get_context();
	return context->uid;
}

gid_t get_gid() {
	struct fuse_context	*context;
	
	context = fuse_get_context();
	return context->gid;
}

pid_t get_caller_pid() {
	struct fuse_context	*context;
	
	context = fuse_get_context();
	return context->pid;
}

static int hexPad(char *dest, unsigned char x) {
	char	*d;
	
	d = dest;
	sprintf(dest, "%2x", x);
	if (*d == ' ') {
		*d = '0';
	}
	return 2;
}

uint64_t offsetToBlock(off_t offset) {
	return (uint64_t)(offset / SRFS_BLOCK_SIZE);
}

// for debugging, testing purposes only
void bytesToString(char *dest, unsigned char *src, int length) {
	int	i;
	
	for (i = 0; i < length; i++) {
		//dest += sprintf(dest, "%2x", src[i]);
		dest += hexPad(dest, src[i]);
		if ((i + 1) % 4 == 0) {
			dest += sprintf(dest, ":");
		}
	}
}

// temporary
int is_writable_path(const char *path) {
	return !strncmp(path, SKFS_WRITE_BASE, SKFS_WRITE_BASE_LENGTH);
}

int is_base_path(const char *path) {
	return !strcmp(path, SKFS_BASE);
}