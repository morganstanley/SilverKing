// FileIDToPathMap.c

/////////////
// includes

#include "FileAttr.h"
#include "FileIDToPathMap.h"
#include "SRFSConstants.h"
#include "Util.h"

#include <string.h>


////////////////////
// private members

//static int _f2pMinHashSize = 1024;
// using larger size until ht expand is fixed
static int _f2pMinHashSize = 128 * 1024;


///////////////////
// implementation

FileIDToPathMap *f2p_new() {
    FileIDToPathMap *f2p;

    f2p = (FileIDToPathMap *)mem_alloc(1, sizeof(FileIDToPathMap));
    f2p->ht = create_hashtable(_f2pMinHashSize, (unsigned int (*)(void *))fid_hash, (int (*)(void *a, void *b))fid_compare);
    pthread_rwlock_init(&f2p->rwLock, 0); 
    return f2p;
}

void f2p_delete(FileIDToPathMap **f2p) {
    if (f2p != NULL && *f2p != NULL) {
        pthread_rwlock_wrlock(&(*f2p)->rwLock);
        hashtable_destroy((*f2p)->ht, 1);
        pthread_rwlock_unlock(&(*f2p)->rwLock);
        pthread_rwlock_destroy(&(*f2p)->rwLock);
        mem_free((void **)f2p);
    } else {
        fatalError("bad ptr passed to f2p_delete");
    }
}

void f2p_put(FileIDToPathMap *f2p, FileID *fid, char *_path) {
    char            *path;
    PathListEntry    *pathListEntry;

    srfsLog(LOG_FINE, "f2p_put %s", _path);
    path = strdup(_path);
    pthread_rwlock_wrlock(&f2p->rwLock);
    if (srfsLogLevelMet(LOG_FINE)) {
        char    _fid[SRFS_MAX_PATH_LENGTH];

        fid_to_string(fid, _fid);
        srfsLog(LOG_FINE, "f2p_put inserting %llx %s  %s", fid, _fid, path);
    }
    pathListEntry = (PathListEntry *)hashtable_search(f2p->ht, (void *)fid); 
    if (pathListEntry != NULL) {
        hashtable_remove(f2p->ht, (void *)fid);
    }
    hashtable_insert(f2p->ht, fid, ple_prepend(pathListEntry, path)); 
    pthread_rwlock_unlock(&f2p->rwLock);
}

PathListEntry *f2p_get(FileIDToPathMap *f2p, FileID *fid) {
    PathListEntry    *pathListEntry;

    srfsLog(LOG_FINE, "f2p_get");
    pthread_rwlock_rdlock(&f2p->rwLock);
    pathListEntry = (PathListEntry *)hashtable_search(f2p->ht, (void *)fid); 
    pthread_rwlock_unlock(&f2p->rwLock);
    if (srfsLogLevelMet(LOG_FINE)) {
        char    _fid[SRFS_MAX_PATH_LENGTH];
        char    *entry;

        fid_to_string(fid, _fid);
        if (pathListEntry != NULL) {
            entry = pathListEntry->path;
        } else {
            entry = "NULL_PathListEntry";
        }
        srfsLog(LOG_FINE, "f2p_get returning %llx %s %s", fid, _fid, entry);
    }
    return pathListEntry;
}

void f2p_display_stats(FileIDToPathMap *f2p) {
    srfsLog(LOG_WARNING, "f2p size %d", hashtable_count(f2p->ht));
}
