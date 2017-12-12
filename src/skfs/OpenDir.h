// OpenDir.h

#ifndef _OPEN_DIR_H_
#define _OPEN_DIR_H_

/////////////
// includes

#include "DirData.h"
#include "OpenDirUpdate.h"

#include <pthread.h>
#include <stdio.h>


//////////////////
// defines

//#define ODUT_ADDITION 0
//#define ODUT_REMOVAL 2


//////////
// types

// This implementation of OpenDir is a placeholder.
// After functionality of the system is verified, it should
// be replaced with a more efficient implementation.

typedef struct OpenDir {
	char	path[SRFS_MAX_PATH_LENGTH];
	DirData	*dd;
	int		queuedForWrite;
	volatile int	numPendingUpdates;
	OpenDirUpdate	*pendingUpdates;
	pthread_mutex_t	mutexInstance;
	pthread_mutex_t	*mutex;
	pthread_cond_t	cvInstance;
	pthread_cond_t	*cv;
	uint64_t	lastUpdateMillis;
	uint64_t	ddVersion;
	uint64_t	lastMergedVersion;
	uint64_t	lastGetAttr;
	uint64_t	lastPrefetch;
    uint64_t    lastWriteMillis;
	int		needsReconciliation;
} OpenDir;


//////////////////////
// public prototypes

OpenDir *od_new(const char *path, DirData *dd);
void od_delete(OpenDir **od);
uint64_t od_getLastUpdateMillis(OpenDir *od);
uint64_t od_getElapsedSinceLastUpdateMillis(OpenDir *od);
uint64_t od_getLastWriteMillis(OpenDir *od);
void od_setLastWriteMillis(OpenDir *od, uint64_t lastWriteMillis);
void od_waitForWrite(OpenDir *od, uint64_t writeTimeMillis);
void od_mark_deleted(OpenDir *od);
DirData *od_get_DirData(OpenDir *od, int clearPending = FALSE);
void od_rm_entry(OpenDir *od, char *name, uint64_t version);
void od_add_entry(OpenDir *od, char *name, uint64_t version);
int od_add_DirData(OpenDir *od, DirData *dd, SKMetaData *metaData);
int od_updates_pending(OpenDir *od);
int od_set_queued_for_write(OpenDir *od, int queuedForWrite);
void od_display(OpenDir *od, FILE *file = stdout);
int od_record_get_attr(OpenDir *od, char *child, uint64_t curTime);

#endif /* _OPEN_DIR_H_ */
