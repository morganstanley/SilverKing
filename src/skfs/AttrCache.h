// AttrCache.h

#ifndef _ATTR_CACHE_H_
#define _ATTR_CACHE_H_

/////////////
// includes

#include "ActiveOpRef.h"
#include "Cache.h"
#include "FileAttr.h"

#include <sys/types.h>
#include <unistd.h>


//////////
// types

typedef struct AttrCache {
	int	numSubCaches;
	Cache	**attrCaches;
} AttrCache;


//////////////////////
// public prototypes

AttrCache *ac_new(int numSubCaches = 1, int size = CACHE_UNLIMITED_SIZE, int evictionBatchSize = 0);
void ac_delete(AttrCache **fCache);
void ac_remove(AttrCache *aCache, char *path);
CacheReadResult ac_read(AttrCache *aCache, char *path, FileAttr *fa, ActiveOpRef **activeOpRef, void *attrReader, uint64_t minModificationTimeMicros = 0);
CacheReadResult ac_read_no_op_creation(AttrCache *aCache, char *path, FileAttr *fa);
//void ac_store_dht_value(AttrCache *aCache, char *path, SKVal *pRVal);
CacheStoreResult ac_store_raw_data(AttrCache *aCache, char *path, FileAttr *fa, int replace = FALSE, uint64_t modificationTimeMicros = CACHE_NO_MODIFICATION_TIME, uint64_t timeoutMillis = CACHE_NO_TIMEOUT);
void ac_store_active_op(AttrCache *aCache, char *path, ActiveOp *op);
void ac_remove_active_op(AttrCache *aCache, char *path, int fatalErrorOnNotFound = FALSE);
void ac_store_error(AttrCache *aCache, char *path, int errorCode, uint64_t modificationTimeMicros = CACHE_NO_MODIFICATION_TIME, uint64_t timeoutMillis = CACHE_NO_TIMEOUT, int notifyActiveOps_noStorage = FALSE);
void ac_display_stats(AttrCache *aCache);

#endif
