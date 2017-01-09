// G2TaskOutputReader.c

/////////////
// includes

#include "G2OutputDir.h"
#include "G2TaskOutputReader.h"
#include "PathGroup.h"
#include "SRFSConstants.h"
#include "SRFSDHT.h"
#include "Util.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h> 

/*
 G2TaskOutputReader 
 */


////////////////////
// private defines

#define SRFS_OUTPUT_SIZE_BUF_SIZE	128
#define SRFS_COMMAND_BUF_SIZE	8192
#define SFS_READ_SIZE "FILE_SIZE"
#define SFS_READ_CONTENT "FILE_CONTENT"

#define G2_SFS_PORT 5733
#define G2_SFS_BLOCK_SIZE 4096
#define G2_SFS_ST_BLOCK_DIVISOR 196

#define G2_SFS_CONNECT_TIMEOUT_SECONDS 5

//////////////////
// private types


///////////////////////
// private prototypes

static int g2tor_sfs_read(G2TaskOutputReader *g2tor, char *host, int port, 
				   char *fileName, char *sfsRequest, char *buffer, int bufferSize);


////////////////////
// private members

static int _cacheMinHashSize = 1024;
static char *suffix[] = {".stdout", ".stderr"};
static const char * _jobTaskIDs = "TaskIDs";

///////////////////
// implementation

G2TaskOutputReader *g2tor_new(PathGroup *taskOutputPaths, int taskOutPort, char * hostName) {
	G2TaskOutputReader	*g2tor;

	g2tor = (G2TaskOutputReader *)mem_alloc(1, sizeof(G2TaskOutputReader));
	g2tor->taskOutputPaths = taskOutputPaths;
    g2tor->dirHT = create_hashtable(_cacheMinHashSize, 
									(unsigned int (*)(void *))stringHash, 
									(int(*)(void *, void *))strcmp);
	g2tor->dirStat = (struct stat *)mem_alloc(1, sizeof(struct stat));
	g2tor->dirStat->st_mode = S_IFDIR | 0755;
	g2tor->dirStat->st_nlink = 2;
	g2tor->dirStat->st_atime = BIRTHDAY_1;
	g2tor->dirStat->st_mtime = BIRTHDAY_2;
	g2tor->dirStat->st_ctime = BIRTHDAY_3;
	g2tor->regStat = (struct stat *)mem_alloc(1, sizeof(struct stat));
	g2tor->regStat->st_mode = S_IFREG | 0444;
	g2tor->regStat->st_atime = BIRTHDAY_1;
	g2tor->regStat->st_mtime = BIRTHDAY_4;
	g2tor->regStat->st_ctime = BIRTHDAY_5;
	g2tor->taskOutputPort    = taskOutPort;
	g2tor->host              = hostName;
    pthread_rwlock_init(&g2tor->rwLock, 0); 
	return g2tor;
}

void g2tor_delete(G2TaskOutputReader **g2tor) {
	if (g2tor != NULL && *g2tor != NULL) {
		hashtable_destroy((*g2tor)->dirHT, TRUE);
		mem_free((void **)&(*g2tor)->dirStat);
		mem_free((void **)&(*g2tor)->regStat);
		pthread_rwlock_destroy(&(*g2tor)->rwLock);
		mem_free((void **)g2tor);
	} else {
		fatalError("bad ptr in g2tor_delete");
	}
}

static int connectSocket(char *host, int port, int timeoutSeconds) {
    char *addr;                  /* will be a pointer to the address */ 
    struct sockaddr_in address;  /* the libc network address data structure */ 
    fd_set fdset; 
    struct timeval tv; 
    struct sockaddr_in	serv_addr;
    struct hostent		*server;
    int	sockfd;
  
    server = gethostbyname(host);
    if (server == NULL) {
		srfsLog(LOG_WARNING, "Can't resolve host");
		return -1;
    }
    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, 
         (char *)&serv_addr.sin_addr.s_addr,
         server->h_length);
    serv_addr.sin_port = htons(port);

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd < 0) {
		srfsLog(LOG_WARNING, "ERROR opening socket %s %d", host, port);
        return -1;
	}
    //fcntl(sockfd, F_SETFL, O_NONBLOCK); 
 
	if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        printf("ERROR connecting\n");
		srfsLog(LOG_WARNING, "ERROR connecting %s %d", host, port);
		return -1;
	}
 
    FD_ZERO(&fdset); 
    FD_SET(sockfd, &fdset); 
    tv.tv_sec = timeoutSeconds;
    tv.tv_usec = 0; 
 
    if (select(sockfd + 1, NULL, &fdset, NULL, &tv) == 1) { 
        int so_error; 
        socklen_t len = sizeof so_error; 
 
        getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &so_error, &len); 
        if (so_error == 0) { 
            //printf("%s:%d is open\n", addr, port); 
		} else {
			close(sockfd);
			sockfd = -1;
		}
    }  
	return sockfd;
}


static int g2tor_sfs_read(G2TaskOutputReader *g2tor, char *host, int port, 
						  char *fileName, char *sfsRequest, char *buffer, int bufferSize) {
	int	n;
	char	commandBuf[SRFS_COMMAND_BUF_SIZE];
	int	totalRead;
	unsigned int	totalWritten = 0;
	int	sockfd;

	sockfd = connectSocket(host, port, G2_SFS_CONNECT_TIMEOUT_SECONDS);

    bzero(commandBuf, SRFS_COMMAND_BUF_SIZE);
    sprintf(commandBuf, "%s %s\n", sfsRequest, fileName);
	do {
		n = write(sockfd, commandBuf, strlen(commandBuf));
		if (n < 0)  {
			srfsLog(LOG_WARNING, "ERROR writing to socket");
			close(sockfd);
			return -1;
		} else {
			totalWritten += n;
		}
	} while (totalWritten < strlen(commandBuf) && n > 0);
	totalRead = 0;
	do {
		n = read(sockfd, buffer + totalRead, bufferSize);
		if (n < 0) {
			srfsLog(LOG_WARNING, "ERROR reading from socket");
			close(sockfd);
			return -1;
		} else {
			totalRead += n;
		}
	} while (totalRead < bufferSize && n > 0);
    close(sockfd);
	(void) g2tor; //fix for "unused parameter" warning
	return totalRead;
}

static off_t g2tor_read_length(G2TaskOutputReader *g2tor, char *host, int port, char *fileName) {
	char	sizeBuf[SRFS_OUTPUT_SIZE_BUF_SIZE];
	off_t	fileSize;
	int		rc;

	rc = g2tor_sfs_read(g2tor, host, port, fileName, SFS_READ_SIZE, sizeBuf, SRFS_OUTPUT_SIZE_BUF_SIZE);
	if (rc >= 0) {
		fileSize = atoi(sizeBuf);
		return fileSize;
	} else {
		return rc;
	}
}

int g2tor_read_content(G2TaskOutputReader *g2tor, char *host, int port, char *fileName, off_t offset, int length, char *dest) {
	char	filePathAndOffsets[SRFS_COMMAND_BUF_SIZE];

	srfsLog(LOG_FINE, "g2tor_read_content %s from location %s:%d %ld %d", fileName, host, port, offset, length);
	if (dest == NULL) {
		fatalError("NULL dest", __FILE__, __LINE__);
	}
	sprintf(filePathAndOffsets, "%s %lu %lu", fileName, offset, offset + length);
	return g2tor_sfs_read(g2tor, host, port, filePathAndOffsets, SFS_READ_CONTENT, dest, length);
}

OutputDir *g2tor_read_task_ids(G2TaskOutputReader *g2tor, char *jobUUID) {
	SKOperationState::SKOperationState	rc;
	OutputDir	        *od = NULL;
    StrVector           requestGroup;

	(void) g2tor; //fix for "unused parameter" warning
    requestGroup.push_back(_jobTaskIDs);
    SKNamespace * pNs = pGlobalSession->createNamespace(jobUUID);
    SKAsyncNSPerspective * ansp = pGlobalSession->openAsyncNamespacePerspective(jobUUID);
    SKAsyncValueRetrieval * pValRetrieval = ansp->get(&requestGroup);
	//FIXME : consider changing wait  to (some form of) looping thru results?
    pValRetrieval->waitForCompletion();
    rc = pValRetrieval->getState();
    od = NULL;
	if (rc == SKOperationState::SUCCEEDED) {
        StrValMap * pVals = pValRetrieval->getValues();
        if ( pVals != NULL ) {
            SKVal *pval = pVals->at(_jobTaskIDs);
		    //printf("%s", val.c_str());
            if( pval  ) {
                if ( pval->m_len > 0 )
		            od = g2od_parse((char *) pval->m_pVal, pval->m_len);
                sk_destroy_val(&pval);
            }
            delete pVals;
        } 
	} 
    pValRetrieval->close();
    delete pValRetrieval;
    delete pNs;
    delete ansp;
    return od;
}

static int g2tor_read_task_location(G2TaskOutputReader *g2tor, char *jobUUID, char *taskID, char *location) {
	SKOperationState::SKOperationState	rc;
	int		            result = FALSE;
    StrVector           requestGroup;

	(void) g2tor; //fix for "unused parameter" warning
    requestGroup.push_back(taskID);
    //pSession->createNamespace(jobUUID);  //FIXME: keep/remove
    SKAsyncNSPerspective * ansp = pGlobalSession->openAsyncNamespacePerspective(jobUUID);
    SKAsyncValueRetrieval * pValRetrieval = ansp->get(&requestGroup);
    pValRetrieval->waitForCompletion();
    rc = pValRetrieval->getState();
	if (rc == SKOperationState::SUCCEEDED) {
        StrValMap * pVals = pValRetrieval->getValues();
        if ( pVals != NULL ) {
            SKVal * pval = pVals->at(taskID);
            if(pval ) {
                if(pval->m_len > 0) {
                    memcpy(location, pval->m_pVal, pval->m_len);
                    srfsLog(LOG_FINE, "task location %s", location);
		            result = TRUE;
                }
                sk_destroy_val(&pval);
            }
            delete pVals;
        }
	}
    pValRetrieval->close();
    delete pValRetrieval;
    return result;
}

static int g2tor_read_task_hostname(G2TaskOutputReader *g2tor, char *jobUUID, char *taskID, char *hostname, OutputDir *od) {
	int	result;

	result = g2tor_read_task_location(g2tor, jobUUID, taskID, hostname);
	if (result) {
		char	*c;

		c = strchr(hostname, ':');
		if (c != NULL) {
			*c = '\0';
			g2od_set_hostname(od, taskID, hostname);
		}
	}
	return result;
}

void g2tor_test(G2TaskOutputReader *g2tor, char *jobUUID) {
	OutputDir	*od;

	od = g2tor_read_task_ids(g2tor, jobUUID);
	if (!od) {
		fatalError("g2tor_test failed", __FILE__, __LINE__);
	}
	g2od_display(od);
	g2od_delete(&od);
}

int g2tor_readdir(G2TaskOutputReader *g2tor, char *path, void *buf, 
				  fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
	OutputDir	*od;
	int			i;
	int			pathDepth;
	char		*s1;
	char		jobUUID[SRFS_MAX_PATH_LENGTH];

	(void) offset; (void) fi; //fix for "unused parameter" warning
	srfsLog(LOG_FINE, "g2tor_readdir %s", path);
	pathDepth = strcntc((char *)path, '/');
	switch (pathDepth) {
		case 0: srfsLog(LOG_FINE, "bogus path %s", path); return -ENOENT;
		case 1: srfsLog(LOG_FINE, "unexpected depth for g2tor_readdir"); return -ENOENT;
		case 2: break; // a jobUUID path
		default: srfsLog(LOG_FINE, "unsupported path %s", path); return -ENOENT;
	}

	s1 = strchr(path + 1, '/');
	if (s1 == NULL) {
		fatalError("panic", __FILE__, __LINE__);
	}
	memset(jobUUID, 0, SRFS_MAX_PATH_LENGTH);
	strcpy(jobUUID, s1 + 1);

	od = g2tor_get_dir(g2tor, jobUUID, FALSE);
	if (!od) {
		srfsLog(LOG_FINE, "Couldn't find od for %s", jobUUID);
		return -ENOENT;
	} else {
		srfsLog(LOG_FINE, "Found od for %s", jobUUID);
		for (i = 0; i < od->numEntries; i++) {
			char	outputFileName[SRFS_MAX_PATH_LENGTH];
			struct stat st;

			memset(&st, 0, sizeof(st));
			st.st_ino = od->entries[i]->ino;
			st.st_mode = S_IFREG | 0444;
			st.st_atime = BIRTHDAY_1;
			st.st_mtime = BIRTHDAY_2;
			st.st_ctime = BIRTHDAY_3;
			sprintf(outputFileName, "%s.stdout", od->entries[i]->name);
			if (filler(buf, outputFileName, &st, 0)) {
				break;
			}
			sprintf(outputFileName, "%s.stderr", od->entries[i]->name);
			if (filler(buf, outputFileName, &st, 0)) {
				break;
			}
		}
	}
    return 0;
}

OutputDir *g2tor_get_dir(G2TaskOutputReader *g2tor, char *jobUUID, int fetchIfNotFound) {
	OutputDir	*od;

    pthread_rwlock_rdlock(&g2tor->rwLock);
	srfsLog(LOG_FINE, "g2tor %lx %lx searching for dir %s", g2tor, g2tor->dirHT, jobUUID);
	od = (OutputDir *)hashtable_search(g2tor->dirHT, (void *)jobUUID); 
	srfsLog(LOG_FINE, "g2tor od %s %lx", jobUUID, od);
	pthread_rwlock_unlock(&g2tor->rwLock);
	if (od == NULL && fetchIfNotFound) {
		od = g2tor_read_task_ids(g2tor, jobUUID);
		srfsLog(LOG_FINE, "g2tor_read_task_ids result %s %lx", jobUUID, od);
		if (od != NULL) {
			OutputDir	*existingOD;

			pthread_rwlock_wrlock(&g2tor->rwLock);
			existingOD = (OutputDir *)hashtable_search(g2tor->dirHT, (void *)jobUUID); 
			if (existingOD == NULL) {
				hashtable_insert(g2tor->dirHT, str_dup(jobUUID), od); 
				pthread_rwlock_unlock(&g2tor->rwLock);
				srfsLog(LOG_FINE, "inserted new output dir %s %lx", jobUUID, od);
			} else {
				pthread_rwlock_unlock(&g2tor->rwLock);
				g2od_delete(&od);
				od = existingOD;
				srfsLog(LOG_FINE, "found existing output dir %s %lx", jobUUID, od);
			}
		}
	}
	return od;
}

static int g2tor_get_job_path_attr(G2TaskOutputReader *g2tor, char *path, struct stat *stbuf) {
	OutputDir	*od;
	char		*s1;

	srfsLog(LOG_FINE, "g2tor_get_job_path_attr %s", path);
	s1 = strchr(path + 1, '/');
	srfsLog(LOG_FINE, "%c %d", *s1, s1 - path);
	if (pg_matches(g2tor->taskOutputPaths, path, s1 - path)) {
		char	jobUUID[SRFS_MAX_PATH_LENGTH];

		memset(jobUUID, 0, SRFS_MAX_PATH_LENGTH);
		strcpy(jobUUID, s1 + 1);
		srfsLog(LOG_FINE, "jobUUID %s\n", jobUUID);
		od = g2tor_get_dir(g2tor, jobUUID, TRUE);
		if (!od) {
			return ENOENT;
		} else {
			memcpy(stbuf, g2tor->dirStat, sizeof(struct stat));
			return 0;
		}
	} else {
		return ENOENT;
	}
}

static off_t g2tor_get_task_output_length(G2TaskOutputReader *g2tor, char *jobUUID, char *taskID, int mode) {
	OutputDir	*od;
	off_t		length;

	od = g2tor_get_dir(g2tor, jobUUID, FALSE);
	length = 0;
	if (od == NULL) {
		srfsLog(LOG_WARNING, "g2tor_get_task_output_length od not found %s", jobUUID);
	} else {
		length = g2od_get_file_length(od, taskID, mode);
		if (length == G2OD_LENGTH_NO_SUCH_TASK_ID) { 
			srfsLog(LOG_WARNING, "g2tor_get_task_output_length no such taskID %s %s", jobUUID, taskID);
			length = 0;
		} else if (length == G2OD_LENGTH_NOT_SET) { 
			char	hostname[SRFS_MAX_PATH_LENGTH];
			int		result;

			result = g2tor_read_task_hostname(g2tor, jobUUID, taskID, hostname, od);
			if (result == TRUE) {
				char	fileName[SRFS_MAX_PATH_LENGTH];

				sprintf(fileName, "/%s/%s%s", jobUUID, taskID, suffix[mode]);
				srfsLog(LOG_FINE, "Reading srfs %s %d %s", hostname, G2_SFS_PORT, fileName);
				length = g2tor_read_length(g2tor, hostname, G2_SFS_PORT, fileName);
				srfsLog(LOG_FINE, "length %d", length);
				if (length < 0) {
					length = 0;
				} else {
					g2od_set_file_length(od, taskID, length, mode);
				}
			} else {
				length = 0;
			}
		}
	}
	return length;
}

static int g2tor_task_path_to_ids(G2TaskOutputReader *g2tor, char *path, char *jobUUID, char *taskID) {
	char	*s1;
	char	*s2;
	char	*s3;
	int		mode;

	(void) g2tor; //fix for "unused parameter" warning
	memset(jobUUID, 0, SRFS_MAX_PATH_LENGTH);
	memset(taskID, 0, SRFS_MAX_PATH_LENGTH);
	s1 = strchr(path + 1, '/');
	s2 = strchr(s1 + 1, '/');
	memcpy(jobUUID, s1 + 1, s2 - (s1 + 1));
	s3 = strstr(s2 + 1, suffix[G2OD_STDOUT]);
	if (s3 == NULL) {
		s3 = strstr(s2 + 1, suffix[G2OD_STDERR]);
		if (s3 == NULL) {
			mode = -1;
		} else {
			mode = G2OD_STDERR;
		}
	} else {
		mode = G2OD_STDOUT;
	}
	if (mode != -1) {
		memcpy(taskID, s2 + 1, s3 - (s2 + 1));
		srfsLog(LOG_FINE, "jobUUID %s taskID %s", jobUUID, taskID);
	}
	return mode;
}

int g2tor_is_g2tor_path(G2TaskOutputReader *g2tor, const char *path) {
	const char	*s1;

	srfsLog(LOG_FINE, "g2tor_is_g2tor_path %s", path);
	s1 = strchr(path + 1, '/');
	if (s1 != NULL) {
		srfsLog(LOG_FINE, "%c %d", *s1, s1 - path);
		return pg_matches(g2tor->taskOutputPaths, (char *)path, s1 - path);
	} else {
		return FALSE;
	}
}

static int g2tor_get_task_path_attr(G2TaskOutputReader *g2tor, char *path, struct stat *stbuf) {
	OutputDir	*od;
	char		*s1;

	srfsLog(LOG_FINE, "g2tor_get_task_path_attr %s", path);
	s1 = strchr(path + 1, '/');
	srfsLog(LOG_FINE, "%c %d", *s1, s1 - path);
	if (pg_matches(g2tor->taskOutputPaths, path, s1 - path)) {
		char	jobUUID[SRFS_MAX_PATH_LENGTH];
		char	taskID[SRFS_MAX_PATH_LENGTH];
		int		mode;

		mode = g2tor_task_path_to_ids(g2tor, path, jobUUID, taskID);
		if (mode == -1) {
			return ENOENT;
		}
		od = g2tor_get_dir(g2tor, jobUUID, TRUE);
		if (!od) {
			srfsLog(LOG_WARNING, "Can't find OutputDir for known path %s", path);
			return ENOENT;
		} else {
			off_t	length;

			srfsLog(LOG_FINE, "Looking for %s", s1);
			length = g2tor_get_task_output_length(g2tor, jobUUID, taskID, mode);
			memcpy(stbuf, g2tor->regStat, sizeof(struct stat));
			stbuf->st_nlink = 1;
			stbuf->st_blksize = G2_SFS_BLOCK_SIZE;
			stbuf->st_size = length;
			if (length % G2_SFS_ST_BLOCK_DIVISOR == 0) {
				stbuf->st_blocks = length / G2_SFS_ST_BLOCK_DIVISOR;
			} else {
				stbuf->st_blocks = length / G2_SFS_ST_BLOCK_DIVISOR + 1;
			}
			return 0;
		}
	} else {
		return ENOENT;
	}
}

int g2tor_get_attr(G2TaskOutputReader *g2tor, char *path, struct stat *stbuf) {
	int			pathDepth;

	pathDepth = strcntc(path, '/');
	switch (pathDepth) {
		case 0: srfsLog(LOG_WARNING, "bogus path %s", path); return ENOENT;
		case 1: srfsLog(LOG_FINE, "will be handled by cache"); return ENOENT;
		case 2: return g2tor_get_job_path_attr(g2tor, path, stbuf);
		case 3: return g2tor_get_task_path_attr(g2tor, path, stbuf);
		default: srfsLog(LOG_FINE, "unsupported path %s", path); return ENOENT;
	}
}

static int g2tor_read_output(G2TaskOutputReader *g2tor, char *path, char *dest, size_t readSize, off_t readOffset) {
	char	*outputPath;

	srfsLog(LOG_FINE, "g2tor_read_output %lx", dest);
	outputPath = strchr(path + 1, '/');
	if (outputPath != NULL && outputPath[1] != '\0') {
		char	*hostname;
		char	jobUUID[SRFS_MAX_PATH_LENGTH];
		char	taskID[SRFS_MAX_PATH_LENGTH];
		int		mode;

		outputPath++;
		mode = g2tor_task_path_to_ids(g2tor, path, jobUUID, taskID);
		if (mode != -1) {
			OutputDir	*od;

			od = g2tor_get_dir(g2tor, jobUUID, FALSE);
			if (od != NULL) {
				off_t	fileLength;

				fileLength = g2od_get_file_length(od, taskID, mode);
				if (fileLength == G2OD_LENGTH_NOT_SET) {
					// FUSE guarantees that attributes will have been retrieved first
					// and that should set the length
					fatalError("Unexpected length not set", __FILE__, __LINE__); 
				}
				hostname = g2od_get_hostname(od, taskID);
				if (hostname != NULL) {
					int	readLength;

					if (readOffset + (off_t)readSize > fileLength) {
						readLength = fileLength - readOffset;
					} else {
						readLength = readSize;
					}
					return g2tor_read_content(g2tor, hostname, G2_SFS_PORT, outputPath, readOffset, readLength, dest);
				} else {
					srfsLog(LOG_WARNING, "Can't find task location for path %s", path);
					return 0;
				}
			} else {
				srfsLog(LOG_WARNING, "Unexpected output dir not found %s", path);
				return 0;
			}
		} else {
			srfsLog(LOG_WARNING, "Can't parts ids for path %s", path);
			return 0;
		}
	} else {
		return 0;
	}
}

int g2tor_read(G2TaskOutputReader *g2tor, const char *path, char *dest, size_t readSize, off_t readOffset) {
	int			pathDepth;

	pathDepth = strcntc((char *)path, '/');
	switch (pathDepth) {
		case 0: srfsLog(LOG_WARNING, "bogus path %s", path); return -EIO;
		case 1: srfsLog(LOG_FINE, "tried to read dir"); return -EISDIR;
		case 2: srfsLog(LOG_FINE, "tried to read job dir"); return -EISDIR;
		case 3: return g2tor_read_output(g2tor, (char *)path, dest, readSize, readOffset);
		default: srfsLog(LOG_FINE, "unsupported path %s", path); return -EIO;
	}
}

