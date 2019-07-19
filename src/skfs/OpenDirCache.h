// OpenDirCache.h

#ifndef _OPEN_DIR_CACHE_H_
#define _OPEN_DIR_CACHE_H_

/////////////
// includes

#include "ActiveOpRef.h"
#include "Cache.h"
#include "OpenDir.h"
#include "SRFSConstants.h"


//////////
// types

/**
 * OpenDirCache caches pointers to OpenDir instances. Currently, each SKFS 
 * directory has (at most) a single OpenDir instance in any given skfsd that tracks
 * the directory. These OpenDir instances are currently never reclaimed.
 */
typedef struct OpenDirCache {
    int    numSubCaches;
    Cache    **subCaches;
} OpenDirCache;


//////////////////////
// public prototypes

OpenDirCache *odc_new(char *name, int cacheSize, int cacheEvictionBatch, int numSubCaches);
void odc_delete(OpenDirCache **odCache);
CacheReadResult odc_read(OpenDirCache *odCache, char *path, OpenDir **od, ActiveOpRef **activeOpRef, void *dirDataReader);
CacheReadResult odc_read_no_op_creation(OpenDirCache *odCache, char *path, OpenDir **od);
CacheStoreResult odc_store(OpenDirCache *odCache, char *path, OpenDir *od);
void odc_store_active_op(OpenDirCache *odCache, char *path, ActiveOp *op);
void odc_remove_active_op(OpenDirCache *odCache, char *path, int fatalErrorOnNotFound = FALSE);
void odc_store_error(OpenDirCache *odCache, char *path, int errorCode);
void odc_display_stats(OpenDirCache *odCache);
CacheKeyList odc_key_list(OpenDirCache *odCache);

#endif
