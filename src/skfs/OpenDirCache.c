// OpenDirCache.c

/////////////
// includes

#include "OpenDirCache.h"
#include "DirDataReader.h"
#include "SRFSConstants.h"
#include "Util.h"

#include <string.h>


////////////////////
// private defines

#define ODC_CACHE_NAME    "OpenDirCache"


///////////////////////
// private prototypes

static Cache *odc_select_cache(OpenDirCache *odCache, char *path);


///////////////////
// implementation

OpenDirCache *odc_new(char *name, int cacheSize, int cacheEvictionBatch, int numSubCaches) {
    OpenDirCache    *odCache;
    int        i;
    int         subCacheSize;

    odCache = (OpenDirCache *)mem_alloc(1, sizeof(OpenDirCache));
    srfsLog(LOG_FINE, "odc_new:\t%s %d %d\n", name, cacheSize);
    subCacheSize = cacheSize / numSubCaches;
    odCache->numSubCaches = numSubCaches;
    odCache->subCaches = (Cache **)mem_alloc(numSubCaches, sizeof(Cache *));
    for (i = 0; i < numSubCaches; i++) {
        odCache->subCaches[i] = cache_new(ODC_CACHE_NAME, subCacheSize, cacheEvictionBatch,
            (unsigned int (*)(void *))stringHash, (int(*)(void *, void *))strcmp);
    }
    return odCache;
}

void odc_delete(OpenDirCache **odCache) {
    if (odCache != NULL && *odCache != NULL) {
        int    i;
        
        for (i = 0; i < (*odCache)->numSubCaches; i++) {
            cache_delete(&(*odCache)->subCaches[i]);
        }
        mem_free((void **)(*odCache)->subCaches);
        mem_free((void **)odCache);
    } else {
        fatalError("bad ptr in odc_delete");
    }
}

static Cache *odc_select_cache(OpenDirCache *odCache, char *path) {
    return odCache->subCaches[stringHash(path) % odCache->numSubCaches];
}

CacheReadResult odc_read(OpenDirCache *odCache, char *path, OpenDir **od, ActiveOpRef **activeOpRef, void *dirDataReader) {
    if (srfsLogLevelMet(LOG_FINE)) {
        srfsLog(LOG_FINE, "odc_read %llx %s", path, path);
    }
    return cache_read(odc_select_cache(odCache, path), path, strlen(path) + 1, (unsigned char *)od, 0, sizeof(OpenDir *), activeOpRef, NULL, 
                        ddr_create_active_op, dirDataReader);
}

CacheReadResult odc_read_no_op_creation(OpenDirCache *odCache, char *path, OpenDir **od) {
    if (srfsLogLevelMet(LOG_FINE)) {
        srfsLog(LOG_FINE, "odc_read %llx %s", path, path);
    }
    return cache_read(odc_select_cache(odCache, path), path, strlen(path) + 1, (unsigned char *)od, 0, sizeof(OpenDir *), NULL, NULL, 
                        NULL, NULL);
}

CacheStoreResult odc_store(OpenDirCache *odCache, char *path, OpenDir *od) {
    OpenDir    **_od;
    
    if (srfsLogLevelMet(LOG_FINE)) {
        srfsLog(LOG_FINE, "odc_store_raw_data %llx %s", path, path);
    }
    // For now, we create a permanent pointer and store it in the cache.
    _od = (OpenDir **)mem_alloc(1, sizeof(OpenDir *));
    *_od = od;
    return cache_store_raw_data(odc_select_cache(odCache, path), path, strlen(path) + 1, _od, sizeof(OpenDir *), FALSE, curSKTimeNanos());
}

void odc_store_active_op(OpenDirCache *odCache, char *path, ActiveOp *op) {
    if (srfsLogLevelMet(LOG_FINE)) {
        srfsLog(LOG_FINE, "odc_store_active_op %llx %s", path, path);
    }
    cache_store_active_op(odc_select_cache(odCache, path), path, strlen(path) + 1, op);
}

void odc_remove_active_op(OpenDirCache *odCache, char *path, int fatalErrorOnNotFound) {
    cache_remove_active_op(odc_select_cache(odCache, path), path, fatalErrorOnNotFound);
}

void odc_store_error(OpenDirCache *odCache, char *path, int errorCode) {
    cache_store_error(odc_select_cache(odCache, path), path, strlen(path) + 1, errorCode,
        FALSE, curSKTimeNanos(), CACHE_NO_TIMEOUT);
}

void odc_display_stats(OpenDirCache *odCache) {
    int    i;
    
    for (i = 0; i < odCache->numSubCaches; i++) {
        srfsLog(LOG_WARNING, "subCache %d", i);
        cache_display_stats(odCache->subCaches[i]);
    }
}

CacheKeyList odc_key_list(OpenDirCache *odCache) {
    CacheKeyList    *keyLists;
    CacheKeyList    odcKeyList;
    int                i;
    int                j;
    int                k;

    memset(&odcKeyList, 0, sizeof(CacheKeyList));
    keyLists = (CacheKeyList *)mem_alloc(odCache->numSubCaches, sizeof(CacheKeyList));
    for (i = 0; i < odCache->numSubCaches; i++) {
        keyLists[i] = cache_key_list(odCache->subCaches[i]);
        odcKeyList.size += keyLists[i].size;
        srfsLog(LOG_FINE, "keyLists[%d].size %d keyLists[%d].keys %llx", i, keyLists[i].size, i, keyLists[i].keys);
    }
    odcKeyList.keys = (char **)mem_alloc(odcKeyList.size, sizeof(char *));
    k = 0;
    for (i = 0; i < odCache->numSubCaches; i++) {
        for (j = 0; j < keyLists[i].size; j++) {
            odcKeyList.keys[k] = keyLists[i].keys[j];
            k++;
        }
        if (keyLists[i].size > 0) {
            mem_free((void **)&keyLists[i].keys);
        }
    }
    mem_free((void **)&keyLists);
    return odcKeyList;
}
