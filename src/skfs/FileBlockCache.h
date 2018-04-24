// FileBlockCache.h

#ifndef _FILE_BLOCK_CACHE_H_
#define _FILE_BLOCK_CACHE_H_

/////////////
// includes

#include "ActiveOpRef.h"
#include "Cache.h"
#include "FileBlockID.h"
#include "FileIDToPathMap.h"
#include "SRFSConstants.h"
#include "Util.h"


//////////
// types

typedef struct FileBlockCache {
	int	numSubCaches;
	Cache	*permanentCache;
	Cache	**transientCaches;
	char permanentSuffixes[SRFS_MAX_PERMANENT_SUFFIXES][SRFS_MAX_PATH_LENGTH];
	int numPermanentSuffixes;
	FileIDToPathMap	*f2p;
} FileBlockCache;


//////////////////////
// public prototypes

FileBlockCache *fbc_new(char *name, int transientCacheSize, int transientCacheEvictionBatch, FileIDToPathMap *f2p, int numSubCaches);
void fbc_delete(FileBlockCache **fbCache);
CacheReadResult fbc_read(FileBlockCache *fbCache, FileBlockID *fbid, unsigned char *buf, 
						size_t sourceOffset, size_t size, ActiveOpRef **activeOpRef, int *cacheNumRead, 
                        void *fbReader, uint64_t minModificationTimeMicros,
                        uint64_t newOpTimeoutMillis);
CacheReadResult fbc_read_no_op_creation(FileBlockCache *fbCache, FileBlockID *fbid, 
						 unsigned char *buf, size_t sourceOffset, size_t size, int *cacheNumRead, 
						 uint64_t minModificationTimeMicros);
CacheStoreResult fbc_store_dht_value(FileBlockCache *fbCache, FileBlockID *fbid, SKVal *pRVal,
                                    uint64_t modificationTimeMicros = CACHE_NO_MODIFICATION_TIME);
CacheStoreResult fbc_store_raw_data(FileBlockCache *fbCache, FileBlockID *fbid, void *data, size_t size, int replace = FALSE, uint64_t modificationTimeMicros = CACHE_NO_MODIFICATION_TIME);
void fbc_remove(FileBlockCache *fbCache, FileBlockID *fbid, int removeActiveOps = TRUE);
void fbc_store_active_op(FileBlockCache *fbCache, FileBlockID *fbid, ActiveOp *op);
void fbc_remove_active_op(FileBlockCache *fbCache, FileBlockID *fbid, int fatalErrorOnNotFound = FALSE);
void fbc_store_error(FileBlockCache *fbCache, FileBlockID *key, int errorCode, uint64_t modificationTimeMicros, uint64_t timeoutMillis);
void fbc_parse_permanent_suffixes(FileBlockCache *fbCache, char *permanentSuffixes);
void fbc_display_stats(FileBlockCache *fbCache);

#endif
