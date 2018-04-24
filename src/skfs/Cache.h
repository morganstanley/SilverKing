// Cache.h

#ifndef _CACHE_H_
#define _CACHE_H_

/////////////
// includes

#include "ActiveOpRef.h"
#include "skbasictypes.h"
#include "hashtable.h"
#include "hashtable_itr.h"
#include "Util.h"

#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>


////////////
// defines

#define CACHE_UNLIMITED_SIZE	0
#define CACHE_NO_TIMEOUT	0
#define CACHE_NO_MODIFICATION_TIME 0xffffffffffffffffL

//////////
// types

typedef enum {CACHE_STORE_SUCCESS, CACHE_STORE_ALREADY_PRESENT, CACHE_STORE_OLD_REMOVED_NOTHING_STORED} CacheStoreResult;
// Note: CRR_CODE_TRAILER is used to count the number of codes, not as a code proper
typedef enum {CRR_NOT_FOUND, CRR_FOUND, CRR_ACTIVE_OP_CREATED, CRR_ACTIVE_OP_EXISTING, CRR_ERROR_CODE, CRR_CODE_TRAILER} CacheReadResult;
extern char *crrNames[];
extern char *crr_strings[];

typedef struct CacheStats {
	uint64_t	writes;
	uint64_t	readResults[CRR_CODE_TRAILER];
	uint64_t	specReadResults[CRR_CODE_TRAILER];
	uint64_t	evictions;
	uint64_t	failed_evictions;
	uint64_t	removals;
} CacheStats;

typedef struct Cache {
    const char	*name;
    int			size;
    int			evictionBatchSize;
    hashtable	*ht;
	CacheStats	stats;
	pthread_rwlock_t	rwLock;
	pthread_spinlock_t	statLock;
} Cache;

typedef struct CacheKeyList {
	int		size;
	char	**keys;
} CacheKeyList;


//////////////////////
// public prototypes

Cache *cache_new(char *name, int size, int evictionBatchSize, unsigned int (*hash)(void *), int(*compare)(void *, void *));
void cache_delete(Cache **cache);
//void cache_write_lock(Cache *cache);
//void cache_read_lock(Cache *cache);
//void cache_unlock(Cache *cache);
CacheReadResult cache_read(Cache *cache, void *key, size_t keySize, unsigned char *buf, 
						   size_t sourceOffset, size_t size, ActiveOpRef **activeOpRef, int *cacheNumRead, 
						   ActiveOp *(*createOp)(void *, void *, uint64_t), void *createOpContext,
                           uint64_t minModificationTimeMicros = 0,
                           uint64_t newOpTimeoutMillis = CACHE_NO_TIMEOUT);
CacheStoreResult cache_store_dht_value(Cache *cache, void *key, int keySize, SKVal *pRVal, 
    uint64_t modificationTimeMicros = CACHE_NO_MODIFICATION_TIME, uint64_t timeoutMillis = CACHE_NO_TIMEOUT);
CacheStoreResult cache_store_raw_data(Cache *cache, void *key, int keySize, void *data, size_t length, int replace = FALSE, uint64_t modificationTimeMicros = CACHE_NO_MODIFICATION_TIME, uint64_t timeoutMillis = CACHE_NO_TIMEOUT);
void cache_store_active_op(Cache *cache, void *key, int keySize, ActiveOp *op);
void cache_store_error(Cache *cache, void *key, int keySize, int errorCode, int notifyActiveOps_noStorage /*= FALSE*/, uint64_t modificationTimeMicros, uint64_t timeoutMillis);
void cache_remove(Cache *cache, void *key, int removeActiveOps = TRUE);
void cache_remove_active_op(Cache *cache, void *key, int fatalErrorOnNotFound = FALSE);
void cache_display_stats(Cache *cache);
void cache_unpin(Cache *cache, void *key);
CacheKeyList cache_key_list(Cache *cache);

// FUTURE - Consider removing keysize

#endif
