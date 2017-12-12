// AttrCache.c

/////////////
// includes

#include "AttrCache.h"
#include "AttrReader.h"
#include "Util.h"

#include <string.h>


////////////////////
// private defines

#define AC_CACHE_NAME "AttrCache"


///////////////////////
// private prototypes




///////////////////
// implementation

AttrCache *ac_new(int numSubCaches, int size, int evictionBatchSize) {
	AttrCache	*aCache;
	int		i;
	
	aCache = (AttrCache *)mem_alloc(1, sizeof(AttrCache));
    srfsLog(LOG_FINE, "ac_new:\t%s %d\n", AC_CACHE_NAME, size);
	aCache->numSubCaches = numSubCaches;
	aCache->attrCaches = (Cache **)mem_alloc(numSubCaches, sizeof(Cache *));
	for (i = 0; i < numSubCaches; i++) {
		aCache->attrCaches[i] = cache_new(AC_CACHE_NAME, size, evictionBatchSize,
			(unsigned int (*)(void *))stringHash, (int(*)(void *, void *))strcmp);
	}
	return aCache;
}

void ac_delete(AttrCache **aCache) {
	if (aCache != NULL && *aCache != NULL) {
		int	i;
		
		for (i = 0; i < (*aCache)->numSubCaches; i++) {
			cache_delete(&(*aCache)->attrCaches[i]);
		}
		mem_free((void **)&(*aCache)->attrCaches);
		mem_free((void **)aCache);
	} else {
		fatalError("bad ptr in ac_delete");
	}
}

static Cache *ac_sub_cache(AttrCache *aCache, char *path) {
	return aCache->attrCaches[stringHash(path) % aCache->numSubCaches];
}

void ac_remove(AttrCache *aCache, char *path) {
    cache_remove(ac_sub_cache(aCache, path), path, FALSE);
}

CacheReadResult ac_read(AttrCache *aCache, char *path, FileAttr *fa, ActiveOpRef **activeOpRef, void *attrReader, uint64_t minModificationTimeMicros) {
	return cache_read(ac_sub_cache(aCache, path), path, strlen(path) + 1, (unsigned char *)fa, 0, sizeof(FileAttr), activeOpRef, NULL, 
						ar_create_active_op, attrReader, minModificationTimeMicros);
}

CacheReadResult ac_read_no_op_creation(AttrCache *aCache, char *path, FileAttr *fa) {
	return cache_read(ac_sub_cache(aCache, path), path, strlen(path) + 1, (unsigned char *)fa, 0, sizeof(FileAttr), NULL, NULL, 
						NULL, NULL);
}

//void ac_store_dht_value(AttrCache *aCache, char *path, SKVal *pRVal) {
//	cache_store_dht_value(ac_sub_cache(aCache, path), path, strlen(path) + 1, pRVal);
//}

CacheStoreResult ac_store_raw_data(AttrCache *aCache, char *path, FileAttr *data, int replace, uint64_t modificationTimeMicros, uint64_t timeoutMillis) {
	srfsLog(LOG_FINE, "ac_store_raw_data %s %u %u", path, timeoutMillis, data->stat.st_size);
	return cache_store_raw_data(ac_sub_cache(aCache, path), path, strlen(path) + 1, data, sizeof(FileAttr), replace, modificationTimeMicros, timeoutMillis);
}

void ac_remove_active_op(AttrCache *aCache, char *path, int fatalErrorOnNotFound) {
	cache_remove_active_op(ac_sub_cache(aCache, path), path, fatalErrorOnNotFound);
}

void ac_store_active_op(AttrCache *aCache, char *path, ActiveOp *op) {
	cache_store_active_op(ac_sub_cache(aCache, path), path, strlen(path) + 1, op);
}

void ac_store_error(AttrCache *aCache, char *path, int errorCode, uint64_t modificationTimeMicros, uint64_t timeoutMillis, int notifyActiveOps_noStorage) {
    srfsLog(LOG_FINE, "ac_store_error %s %d", path, errorCode);
	cache_store_error(ac_sub_cache(aCache, path), path, strlen(path) + 1, errorCode, notifyActiveOps_noStorage, modificationTimeMicros, timeoutMillis);
}

void ac_display_stats(AttrCache *aCache) {
	int	i;
	
	for (i = 0; i < aCache->numSubCaches; i++) {
		srfsLog(LOG_WARNING, "subCache %d", i);
		cache_display_stats(aCache->attrCaches[i]);
	}
}
