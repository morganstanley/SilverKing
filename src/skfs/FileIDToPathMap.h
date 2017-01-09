// FileIDToPathMap.h

#ifndef _FILE_ID_TO_PATH_MAP_H_
#define _FILE_ID_TO_PATH_MAP_H_

/////////////
// includes

#include "FileID.h"
#include "hashtable.h"
#include "PathListEntry.h"

#include <pthread.h>


//////////
// types

typedef struct FileIDToPathMap {
    hashtable	*ht;
	pthread_rwlock_t	rwLock;
} FileIDToPathMap;


///////////////
// prototypes

FileIDToPathMap *f2p_new();
void f2p_delete(FileIDToPathMap **f2p);
void f2p_put(FileIDToPathMap *f2p, FileID *fid, char *path);
PathListEntry *f2p_get(FileIDToPathMap *f2p, FileID *fid);

#endif
