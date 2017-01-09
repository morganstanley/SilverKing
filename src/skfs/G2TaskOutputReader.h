// G2TaskOutputReader.h

#ifndef _G2_TASK_OUTPUT_READER_H_
#define _G2_TASK_OUTPUT_READER_H_

/////////////
// includes

#include "hashtable.h"
#include "G2OutputDir.h"
#include "PathGroup.h"

#include <fuse.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>


//////////
// types

typedef struct G2TaskOutputReader {
	PathGroup			*taskOutputPaths;
    hashtable			*dirHT;
	pthread_rwlock_t	rwLock;
	struct stat			*dirStat;
	struct stat			*regStat;
	int                 taskOutputPort;
	char                *host;
} G2TaskOutputReader;


///////////////
// prototypes

G2TaskOutputReader *g2tor_new(PathGroup *taskOutputPaths, int taskOutPort = 0, char * hostName = NULL);
void g2tor_delete(G2TaskOutputReader **g2tor);
int g2tor_read_content(G2TaskOutputReader *g2tor, char *host, int port, char *fileName, off_t offset, int length, char *dest);
void g2tor_test(G2TaskOutputReader *g2tor, char *jobUUID);

int g2tor_get_attr(G2TaskOutputReader *g2tor, char *path, struct stat *stbuf);

int g2tor_readdir(G2TaskOutputReader *g2tor, char *path, void *buf, 
				  fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi);

OutputDir *g2tor_get_dir(G2TaskOutputReader *g2tor, char *jobUUID, int fetchIfNotFound);
int g2tor_read(G2TaskOutputReader *g2tor, const char *path, char *dest, size_t readSize, off_t readOffset);
int g2tor_is_g2tor_path(G2TaskOutputReader *g2tor, const char *path);

#endif
