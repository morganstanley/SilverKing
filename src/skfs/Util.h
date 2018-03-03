// Util.h

#ifndef _UTIL_H_
#define _UTIL_H_

/////////////
// includes

#include <pthread.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <unistd.h>
#include <zlib.h>

#include "skbasictypes.h"
#include "SRFSConstants.h"
#include "SKValueCreator.h"

////////////
// defines

#define TRUE 1
#define FALSE 0
#define offset_to_ptr(B,O) ((void *)(((uint64_t)(B)) + (uint64_t)(O)))
#define ptr_to_offset(B,P) ((uint64_t)(P) - (uint64_t)(B))
#define MEM_DBG_IGNORE_ALLOCATION NULL


//////////
// types

typedef enum {LOG_ERROR, LOG_WARNING, LOG_OPS, LOG_INFO, LOG_FINE} LogLevel;


///////////////////
// public globals

extern char zeroBlock[SRFS_BLOCK_SIZE];
extern uint64_t    myValueCreator;


/////////////////////
// public functions

void srfsLogInitAsync();
void srfsLogFlush(void);
void srfsLogSetFile(char *fileName);
void srfsRedirectStdio();
void srfsLogAsync(LogLevel level, char const * format, ...);
void srfsLog(LogLevel level, char const * format, ...);
void setSRFSLogLevel(LogLevel level);
int srfsLogLevelMet(LogLevel level);

uint64_t curTimeMillis(void);
uint64_t curTimeMicros(void);

void setFatalErrorWarnOnly(int warnOnly);
void fatalError(char *msg, char *file = "", int line = 0);
void resetFatalErrorCount(void);
uint64_t getFatalErrorCount(void);

void dumpCoreAndContinue();

#define mem_alloc(A,B,...) _mem__alloc(A,B,__FILE__,__LINE__)
#define mem_alloc_no_dbg(A,B,...) _mem__alloc(A,B,MEM_DBG_IGNORE_ALLOCATION,0)
#define mem_free(A,...) _mem__free(A,__FILE__,__LINE__)
#define mem_realloc(A,B,C,D...) _mem__realloc(A,B,C,D,__FILE__,__LINE__)
#define mem_dup(A,B,...) _mem__dup(A,B,__FILE__,__LINE__)
#define mem_dup_no_dbg(A,B,...) _mem__dup(A,B,MEM_DBG_IGNORE_ALLOCATION,0)
#define str_dup(A,...) _str__dup(A,__FILE__,__LINE__)
#define str_dup_no_dbg(A,...) _str__dup(A,MEM_DBG_IGNORE_ALLOCATION,0)
#define int_dup(A,...) _int__dup(A,__FILE__,__LINE__)

void *_mem__alloc(size_t nmemb, size_t size, char *file = "", int line = 0);
void _mem__free(void **ptr, char *file = "", int line = 0);
void _mem__realloc(void **ptr, size_t o_nmemb, size_t nmemb, size_t size, char *file = "", int line = 0);
void *_mem__dup(const void *source, int size, char *file = "", int line = 0);
char *_str__dup(const char *source, char *file = "", int line = 0);
int *_int__dup(int *i, char *file = "", int line = 0);
int strcntc(char *s, char c);

char **str_alloc_array(int r, size_t size);
void str_free_array(char ***a, int r);

void mutex_init(pthread_mutex_t *mutex, pthread_mutex_t **mutexPtr);
void cv_init(pthread_cond_t *cv, pthread_cond_t **cvPtr);
void mutex_destroy(pthread_mutex_t **mutexPtr);
void cv_destroy(pthread_cond_t **cvPtr);
void cv_wait_rel(pthread_mutex_t *mutex, pthread_cond_t *cv, uint64_t interval);
void cv_wait_abs(pthread_mutex_t *mutex, pthread_cond_t *cv, uint64_t deadline);

unsigned int stringHash(void *s);
int suffixMatches(const char *s, const char *suffix);
int string_count_occurrences(char *s, int c);
void ensure_null_terminated(char *s, char *limit, char *f, int l);

int is_base_path(char *path);
void path_display_elements(char **pathElements, int numElements);
char **path_split_elements(char *path, int *numElements);
int path_merge_elements(char **elements, int numElements, char *buf, int bufSize);
void path_free_elements(char ***pathElements, int numElements);
int path_simplify(char *path, char *buf, int bufSize);
char *path_prev(char *c);

char *file_read(char *path, size_t *length = NULL);
int file_read_partial(const char *path, char *dest, size_t readSize, off_t readOffset);
size_t trim_in_place(char *s, size_t length);


unsigned int mem_hash(void *m, int size);

void stat_display(struct stat *s, FILE *f = stdout);
uint64_t stat_mtime_micros(struct stat *s);
uint64_t stat_mtime_millis(struct stat *s);

size_t size_max(size_t a, size_t b);
size_t size_min(size_t a, size_t b);
int int_max(int a, int b);
int int_min(int a, int b);
off_t off_max(off_t a, off_t b);
off_t off_min(off_t a, off_t b);
uint64_t uint64_max(off_t a, off_t b);
uint64_t uint64_min(off_t a, off_t b);

int get_num_cpus(void);
int get_pid();
uint64_t getValueCreatorAsUint64(SKValueCreator *vc);

int zlibBuffToBuffDecompress(char *dest, int *destLength, 
						   char* source, int sourceLength);

uid_t get_uid();
gid_t get_gid();
pid_t get_caller_pid();

void bytesToString(char *dest, unsigned char *src, int length);
						   
time_t epoch_time_seconds();
void sleep_random_millis(uint64_t minMillis, uint64_t maxMillis, unsigned int *seedp);

uint64_t offsetToBlock(off_t offset);

int is_writable_path(const char *path);
int is_base_path(const char *path);

#endif
