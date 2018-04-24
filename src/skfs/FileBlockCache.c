// FileBlockCache.c

/////////////
// includes

#include "FileBlockCache.h"
#include "FileBlockReader.h"
#include "SRFSConstants.h"
#include "Util.h"

#include <string.h>


////////////////////
// private defines

	// FUTURE: think about the permanent cache. Has it outlived its usefulness?

#define FBC_PERMANENT_CACHE_NAME	"PermanentFileBlockCache"
#define FBC_TRANSIENT_CACHE_NAME	"TransientFileBlockCache"


///////////////////////
// private prototypes

static int fbc_is_permanent_path(FileBlockCache *fbCache, char *path);
static Cache *fbc_select_cache(FileBlockCache *fbCache, FileBlockID *fbid);


///////////////////
// implementation

FileBlockCache *fbc_new(char *name, int transientCacheSize, int transientCacheEvictionBatch, FileIDToPathMap *f2p, int numSubCaches) {
	FileBlockCache	*fbCache;
	int		i;
	int 		transientSubCacheSize;

	fbCache = (FileBlockCache *)mem_alloc(1, sizeof(FileBlockCache));
    srfsLog(LOG_WARNING, "fbc_new:\t%s %d\n", name, transientCacheSize);
    transientSubCacheSize = transientCacheSize / numSubCaches;
    fbCache->numSubCaches = numSubCaches;
    fbCache->numPermanentSuffixes = 0;
#ifdef _FBC_USE_PERMANENT_CACHE
	fbCache->permanentCache = cache_new(FBC_PERMANENT_CACHE_NAME, CACHE_UNLIMITED_SIZE, 0, 
		(unsigned int (*)(void *))fbid_hash, (int(*)(void *, void *))fbid_compare);
#else
    fbCache->permanentCache = NULL;
#endif
    fbCache->transientCaches = (Cache **)mem_alloc(numSubCaches, sizeof(Cache *));
	for (i = 0; i < numSubCaches; i++) {
		fbCache->transientCaches[i] = cache_new(FBC_TRANSIENT_CACHE_NAME, transientSubCacheSize, transientCacheEvictionBatch,
			(unsigned int (*)(void *))fbid_hash, (int(*)(void *, void *))fbid_compare);
	}
	fbCache->f2p = f2p;
	return fbCache;
}

void fbc_delete(FileBlockCache **fbCache) {
	if (fbCache != NULL && *fbCache != NULL) {
		int	i;
		
        srfsLog(LOG_WARNING, "fbc_delete\n");
#ifdef _FBC_USE_PERMANENT_CACHE
		cache_delete(&(*fbCache)->permanentCache);
#endif
		for (i = 0; i < (*fbCache)->numSubCaches; i++) {
			cache_delete(&(*fbCache)->transientCaches[i]);
		}
		mem_free((void **)&(*fbCache)->transientCaches);
		mem_free((void **)fbCache);
	} else {
		fatalError("bad ptr in fbc_delete");
	}
}

static Cache *fbc_select_cache(FileBlockCache *fbCache, FileBlockID *fbid) {
#ifdef _FBC_USE_PERMANENT_CACHE
	PathListEntry	*pathListEntry;

	pathListEntry = f2p_get(fbCache->f2p, fbid_get_id(fbid));
	if (pathListEntry == NULL || pathListEntry->path == NULL) {
		srfsLog(LOG_ERROR, "f2p_get failed: %llx", fbid);
		fatalError("f2p_get failed");
	}
	if (fbc_is_permanent_path(fbCache, pathListEntry->path)) {
		return fbCache->permanentCache;
	} else {
#endif
        return fbCache->transientCaches[fbid_hash(fbid) % fbCache->numSubCaches]; 
#ifdef _FBC_USE_PERMANENT_CACHE
	}
#endif
}

CacheReadResult fbc_read(FileBlockCache *fbCache, FileBlockID *fbid, 
						 unsigned char *buf, size_t sourceOffset, size_t size, ActiveOpRef **activeOpRef, int *cacheNumRead, 
						 void *fbReader, uint64_t minModificationTimeMicros,
                         uint64_t newOpTimeoutMillis) {
	if (srfsLogLevelMet(LOG_FINE)) {
		char	_fbid[SRFS_MAX_PATH_LENGTH];

		fbid_to_string(fbid, _fbid);
		srfsLog(LOG_FINE, "fbc_read %llx %s", fbid, _fbid);
	}
	return cache_read(fbc_select_cache(fbCache, fbid), fbid, sizeof(FileBlockID), buf, sourceOffset, size, activeOpRef, cacheNumRead, 
					 fbr_create_active_op, fbReader, minModificationTimeMicros,
                     newOpTimeoutMillis);
}


CacheReadResult fbc_read_no_op_creation(FileBlockCache *fbCache, FileBlockID *fbid, 
						 unsigned char *buf, size_t sourceOffset, size_t size, int *cacheNumRead, 
						 uint64_t minModificationTimeMicros) {
	if (srfsLogLevelMet(LOG_FINE)) {
		char	_fbid[SRFS_MAX_PATH_LENGTH];

		fbid_to_string(fbid, _fbid);
		srfsLog(LOG_FINE, "fbc_read_no_op_creation %llx %s", fbid, _fbid);
	}
	return cache_read(fbc_select_cache(fbCache, fbid), fbid, sizeof(FileBlockID), buf, sourceOffset, size, NULL, cacheNumRead, 
					 NULL, NULL, minModificationTimeMicros);
}

CacheStoreResult fbc_store_dht_value(FileBlockCache *fbCache, FileBlockID *fbid, SKVal *pRVal,
                            uint64_t modificationTimeMicros) {
	if (srfsLogLevelMet(LOG_FINE)) {
		char	_fbid[SRFS_MAX_PATH_LENGTH];

		fbid_to_string(fbid, _fbid);
		srfsLog(LOG_FINE, "fbc_store_dht_value %llx %s %u", fbid, _fbid, pRVal->m_len);
	}
	return cache_store_dht_value(fbc_select_cache(fbCache, fbid), fbid, sizeof(FileBlockID), pRVal, 
                                modificationTimeMicros, CACHE_NO_TIMEOUT);
}

CacheStoreResult fbc_store_raw_data(FileBlockCache *fbCache, FileBlockID *fbid, void *data, 
                                size_t size, int replace, uint64_t modificationTimeMicros) {
	if (srfsLogLevelMet(LOG_FINE)) {
		char	_fbid[SRFS_MAX_PATH_LENGTH];

		fbid_to_string(fbid, _fbid);
		srfsLog(LOG_FINE, "fbc_store_raw_data %llx %s", fbid, _fbid);
	}
	return cache_store_raw_data(fbc_select_cache(fbCache, fbid), fbid, sizeof(FileBlockID), data, size, replace, modificationTimeMicros, CACHE_NO_TIMEOUT);
}

void fbc_store_active_op(FileBlockCache *fbCache, FileBlockID *fbid, ActiveOp *op) {
	if (srfsLogLevelMet(LOG_FINE)) {
		char	_fbid[SRFS_MAX_PATH_LENGTH];

		fbid_to_string(fbid, _fbid);
		srfsLog(LOG_FINE, "fbc_store_active_op %llx %s", fbid, _fbid);
	}
	cache_store_active_op(fbc_select_cache(fbCache, fbid), fbid, sizeof(FileBlockID), op);
}

void fbc_remove(FileBlockCache *fbCache, FileBlockID *fbid, int removeActiveOps) {
	cache_remove(fbc_select_cache(fbCache, fbid), fbid, removeActiveOps);
}

void fbc_remove_active_op(FileBlockCache *fbCache, FileBlockID *fbid, int fatalErrorOnNotFound) {
	cache_remove_active_op(fbc_select_cache(fbCache, fbid), fbid, fatalErrorOnNotFound);
}

void fbc_store_error(FileBlockCache *fbCache, FileBlockID *fbid, int errorCode, 
                     uint64_t modificationTimeMicros, uint64_t timeoutMillis) {
	cache_store_error(fbc_select_cache(fbCache, fbid), fbid, sizeof(FileBlockID), errorCode, 
                        FALSE, modificationTimeMicros, timeoutMillis);
}

// path functions

void fbc_parse_permanent_suffixes(FileBlockCache *fbCache, char *permanentSuffixes) {
    char        *cur;
    char        *div;
    char        *next;

    cur = permanentSuffixes;
    while (cur != NULL) {
        div = strchr(cur, ',');
        if (div == NULL) {
            next = NULL;
        } else {
            next = div + 1;
            *div = '\0';
        }
        strcpy(fbCache->permanentSuffixes[fbCache->numPermanentSuffixes], cur);
		srfsLog(LOG_WARNING, "permanentSuffix:\t%s\n", fbCache->permanentSuffixes[fbCache->numPermanentSuffixes]);
		fbCache->numPermanentSuffixes++;
        cur = next;
    }
}

static int fbc_is_permanent_path(FileBlockCache *fbCache, char *path) {
    int	i;

    for (i = 0; i < fbCache->numPermanentSuffixes; i++) {
		if (suffixMatches(path, fbCache->permanentSuffixes[i])) {
			return TRUE;
		}
    }
    return false;
}

void fbc_display_stats(FileBlockCache *fbCache) {
	int	i;
	
#ifdef _FBC_USE_PERMANENT_CACHE
	cache_display_stats(fbCache->permanentCache);
#endif
	for (i = 0; i < fbCache->numSubCaches; i++) {
		srfsLog(LOG_WARNING, "subCache %d", i);
		cache_display_stats(fbCache->transientCaches[i]);
	}
}
