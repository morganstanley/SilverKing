// Cache.c

/////////////
// includes

#include "ActiveOpRef.h"
#include "Cache.h"
#include "Util.h"

#include <stdlib.h>
#include <string.h>


////////////////////
// private defines

//#define _cache_logging_in_critical_section 1
#define _cache_logging_in_critical_section 0
#define _cache_debug_failed_evictions 0
#define _cache_no_expiration	0xffffffffffffffffL
#define _cache_fatal_error_on_double_free   0


/////////////////
// private types

typedef enum {CACHE_RAW_DATA, CACHE_DHT_VALUE, CACHE_ACTIVE_OP, CACHE_ERROR_CODE, CACHE_DELETED_ENTRY} CacheEntryType;

typedef enum {CRM_WARN, CRM_FATAL_ERROR, CRM_ALLOW} CacheReplacementMode;

typedef struct CacheEntry {
	volatile uint64_t	modificationTime;
	volatile uint64_t	expirationTime;
	volatile uint64_t	lastAccess;
	CacheEntryType	type;
	void	*key;
	void	*data;
	size_t	size;
} CacheEntry;

typedef struct LRUList {
	CacheEntry	**lruEntries;
	int			sizeLimit;
	int			curEntries;
} LRUList;

///////////////////////
// private prototypes

static CacheEntry *cache_entry_new(CacheEntryType type, void *key, size_t keySize, void *data, size_t size, uint64_t modificationTime, uint64_t timeoutMillis);
static void cache_entry_delete(CacheEntry **entry, int deleteData = TRUE, 
                               char *file = NULL, int line = 0);
static CacheStoreResult cache_store_entry(Cache *cache, CacheEntry *entry, int alreadyLocked = FALSE, int notifyActiveOps_noStorage = FALSE, CacheReplacementMode replace = CRM_WARN);
static int cache_entry_is_data_type(CacheEntry *entry);
static size_t cache_entry_get_data_size(CacheEntry *entry);
static void cache_display(Cache *cache);
static CacheEntry *cache_evict(Cache *cache, CacheEntry *entry);
static CacheEntry	*_cache_remove(Cache *cache, void *key, int removeActiveOps = TRUE);


////////////////
// public members

char *crr_strings[] = {"CRR_NOT_FOUND", "CRR_FOUND", "CRR_ACTIVE_OP_CREATED", "CRR_ACTIVE_OP_EXISTING", 
					"CRR_ERROR_CODE"};

////////////////////
// private members

static int _cacheMinHashSize = 1024;

///////////////////
// implementation

// cache

Cache *cache_new(char *name, int size, int evictionBatchSize, unsigned int (*hash)(void *), int(*compare)(void *, void *)) {
	Cache	*cache;

	cache = (Cache *)mem_alloc(1, sizeof(Cache));
    srfsLog(LOG_WARNING, "cache_new:\t%s %d\n", name, size);
    cache->name = name;
    cache->size = size;
	if (size > 0) {
		if (evictionBatchSize <= 0 || evictionBatchSize > size) {
			srfsLog(LOG_ERROR, "Bad evictionBatchSize: %d", evictionBatchSize);
			fatalError("Bad evictionBatchSize", __FILE__, __LINE__);
		}
	} else {
		if (evictionBatchSize != 0) {
			srfsLog(LOG_ERROR, "Non-zero evictionBatchSize: %d", evictionBatchSize);
			fatalError("Non-zero evictionBatchSize", __FILE__, __LINE__);
		}
	}
	cache->evictionBatchSize = evictionBatchSize;
    //cache->ht = create_hashtable(_cacheMinHashSize, hash, compare);
    cache->ht = create_hashtable(size, hash, compare);
    pthread_rwlock_init(&cache->rwLock, 0); 
	pthread_spin_init(&cache->statLock, 0);
    srfsLog(LOG_WARNING, "faondf %d", _cache_fatal_error_on_double_free);
	return cache;
}

void cache_delete(Cache **cache) {
	if (cache != NULL && *cache != NULL) {
        srfsLog(LOG_WARNING, "cache_delete %s", (*cache)->name);
		// FUTURE - verify the hashtable and entries delete
		// all current use cases never call this
        {
			pthread_rwlock_wrlock(&(*cache)->rwLock);

			struct hashtable_itr *itr ;
			itr = hashtable_iterator((*cache)->ht);
			int	walkComplete = FALSE;
			while (!walkComplete && itr != NULL) {
				int			notAtEnd;
				CacheEntry	*entry;
				void    	*key;

				key = hashtable_iterator_key(itr);
				if (key == NULL) {
					fatalError("NULL key", __FILE__, __LINE__);
				}
				entry = (CacheEntry *)hashtable_search((*cache)->ht, key);
				if (entry != NULL) {
					cache_entry_delete(&entry);
				}
				notAtEnd = hashtable_iterator_advance(itr);
                walkComplete = !notAtEnd;
			}
            if (itr != NULL) {
                free(itr);	
                itr = NULL;
            }

			hashtable_destroy((*cache)->ht, 0);
			pthread_rwlock_unlock(&(*cache)->rwLock);
        }
        /*
		{
			pthread_rwlock_wrlock(&(*cache)->rwLock);

			struct hashtable_itr *itr ;
			itr = hashtable_iterator((*cache)->ht);
			int	walkComplete = FALSE;
			while (!walkComplete) {
				//int			notAtEnd;
				CacheEntry	*entry;
				void    	*key;

				key = hashtable_iterator_key(itr);
				if (key == NULL) {
					fatalError("NULL key", __FILE__, __LINE__);
				}
				entry = (CacheEntry *)hashtable_remove((*cache)->ht, key);
				if (entry != NULL) {
					cache_entry_delete(&entry);
				}
				//notAtEnd = hashtable_iterator_advance(itr);
				itr = hashtable_iterator((*cache)->ht);
				if (itr == NULL) {
					walkComplete = TRUE;
				}
			}
			free(itr);	
			itr = NULL;

			hashtable_destroy((*cache)->ht, 1);
			pthread_rwlock_unlock(&(*cache)->rwLock);
		}
        */
		pthread_spin_destroy(&(*cache)->statLock);
		pthread_rwlock_destroy(&(*cache)->rwLock);
		mem_free((void **)cache);
	} else {
		fatalError("bad ptr in cache_delete");
	}
}

//void cache_write_lock(Cache *cache) {
//	pthread_rwlock_wrlock(&cache->rwLock);
//}

//void cache_read_lock(Cache *cache) {
//    pthread_rwlock_rdlock(&cache->rwLock);
//}

//void cache_unlock(Cache *cache) {
//	pthread_rwlock_unlock(&cache->rwLock);
//}

CacheReadResult cache_read(Cache *cache, void *key, size_t keySize, unsigned char *buf, size_t sourceOffset, size_t size, 
						   ActiveOpRef **activeOpRef, int *cacheNumRead, 
						   ActiveOp *(*createOp)(void *, void *, uint64_t), void *createOpContext, 
                           uint64_t minModificationTimeMicros, 
                           uint64_t newOpTimeoutMillis) {
	CacheEntry	*entry;
    SKVal		*pRVal;
    CacheReadResult	result;
	size_t		end;
	size_t		numToRead;
	void		*rawData;
	int			speculativeRead;
	uint64_t	_curTimeMillis;

	// FUTURE - ENSURE THAT WE CAN'T COPY OUT OF BOUNDS
	srfsLog(LOG_FINE, "in cacheRead() %s key %llx sourceOffset %d size %d\n", cache->name, key, sourceOffset, size);
	speculativeRead = buf == NULL;
	pRVal = NULL;
	if (activeOpRef != NULL) {
		if (*activeOpRef != NULL) {
			fatalError("cache_read() passed non-NULL activeOpRef");
		}
	}
    pthread_rwlock_rdlock(&cache->rwLock);
    entry = (CacheEntry *)hashtable_search(cache->ht, (void *)key); 
	if (_cache_logging_in_critical_section) {
		srfsLog(LOG_FINE, "ht entry %llx", entry);
	}

    if (entry == NULL) {
		// upgrade to write lock so that we can create an operation to fetch
		// the missing entry
	    pthread_rwlock_unlock(&cache->rwLock);
		// lock not held here, entry could slip in
		pthread_rwlock_wrlock(&cache->rwLock);
		// must recheck now
		entry = (CacheEntry *)hashtable_search(cache->ht, (void *)key); 
		if (_cache_logging_in_critical_section) {
			srfsLog(LOG_FINE, "cache lock upgraded to wrlock");
			srfsLog(LOG_FINE, "ht entry %llx", entry);
		}
	} else {
        if (_cache_logging_in_critical_section) {
            srfsLog(LOG_WARNING, "minModificationTimeMicros %u entry->modificationTime %u <= %d", minModificationTimeMicros, entry->modificationTime,
                minModificationTimeMicros <= entry->modificationTime);
        }
		_curTimeMillis = curTimeMillis();
		if (    entry->type != CACHE_ACTIVE_OP // Consider removing and using times to accomplish this
                && _curTimeMillis <= entry->expirationTime
                && minModificationTimeMicros <= entry->modificationTime) {
			entry->lastAccess = _curTimeMillis; // safe since we are on 64-bit machines
		} else {
			// upgrade to write lock so that we can expire the entry
			pthread_rwlock_unlock(&cache->rwLock);
			// lock not held here, entry could slip in
			pthread_rwlock_wrlock(&cache->rwLock);
			// must recheck now
			entry = (CacheEntry *)hashtable_search(cache->ht, (void *)key); 
			if (entry != NULL) {
				if (_curTimeMillis <= entry->expirationTime
                        && minModificationTimeMicros <= entry->modificationTime) {
					// A new, non-expired entry slipped in
					entry->lastAccess = _curTimeMillis; // safe since we are on 64-bit machines
				} else {
                    cache_evict(cache, entry);
                    entry = NULL;
				}
			}
		}
	}

    if (entry != NULL) {
		if (_cache_logging_in_critical_section) {
			srfsLog(LOG_FINE, "entry->type %d", entry->type);
		}
		if (!speculativeRead) {
			switch (entry->type) {
			case CACHE_RAW_DATA:
				rawData = entry->data;
				end = size_min(sourceOffset + size, entry->size);
				numToRead = end - sourceOffset;
				if (cacheNumRead != NULL) {
					*cacheNumRead = numToRead;
				}
				memcpy(buf, (unsigned char *)rawData + sourceOffset, numToRead);
				result = CRR_FOUND;
				break;
			case CACHE_DHT_VALUE:
				pRVal = (SKVal *)entry->data;
				end = sourceOffset + size;
				if (end > pRVal->m_len) {
					end = pRVal->m_len;
				}
				numToRead = end - sourceOffset;
				if (cacheNumRead != NULL) {
					*cacheNumRead = numToRead;
					if (_cache_logging_in_critical_section) {
						srfsLog(LOG_FINE, "pRVal->m_len %d size %d end %d sourceOffset %d *cacheNumRead %d", pRVal->m_len, size, end, sourceOffset, *cacheNumRead);
					}
				}
				memcpy(buf, (unsigned char *)pRVal->m_pVal + sourceOffset, numToRead);
				result = CRR_FOUND;
				break;
			case CACHE_ACTIVE_OP:
				if (activeOpRef != NULL) {
					ActiveOpRef	*aor;
					
					aor = (ActiveOpRef *)entry->data;
					*activeOpRef = aor_new(aor->ao, __FILE__, __LINE__);
					if (cacheNumRead != NULL) {
						*cacheNumRead = sizeof(ActiveOpRef *);
					}
				}
				result = CRR_ACTIVE_OP_EXISTING;
				break;
			case CACHE_ERROR_CODE:
				if (size >= sizeof(int)) {
					if (sourceOffset != 0) {
						fatalError("sourceOffset != 0", __FILE__, __LINE__);
					}
					rawData = entry->data;
					numToRead = sizeof(int);
					if (cacheNumRead != NULL) {
						*cacheNumRead = numToRead;
					}
					memcpy(buf, (unsigned char *)rawData, numToRead);
					if (cacheNumRead != NULL) {
						*cacheNumRead = numToRead;
					}
				} else {
					// FUTURE: Consider handling this on the caller side, or making this fatal
					// For AttrReader, the stat struct is always >= sizeof(int).
					// For FileBlockReader, errors are always unexpected, so the exact code isn't critical.
					rawData = entry->data;
					srfsLog(LOG_WARNING, "Unable to copy error code %d", *((int *)rawData));
					if (cacheNumRead != NULL) {
						*cacheNumRead = 0;
					}
				}
				result = CRR_ERROR_CODE;
				break;
			default:
				fatalError("detected invalid CacheEntry", __FILE__, __LINE__);
				result = CRR_NOT_FOUND; // suppress the compiler warning
				break;
			}
		} else {
			if (_cache_logging_in_critical_section) {
				srfsLog(LOG_FINE, "NOTE: speculative read. Forcing result = CRR_FOUND");
			}
			result = CRR_FOUND;
		}
    } else {
		if (createOp != NULL && activeOpRef != NULL) {
			ActiveOp	*op;
			CacheEntry	*newEntry;
			ActiveOpRef	*aor;

			if (cacheNumRead != NULL) {
				*cacheNumRead = sizeof(ActiveOpRef *);
			}
			if (createOpContext == NULL) {
				fatalError("createOpContext == NULL", __FILE__, __LINE__);
			}
			//if (activeOpRef == NULL) {
			//	fatalError("activeOpRef == NULL", __FILE__, __LINE__);
			//}
			result = CRR_ACTIVE_OP_CREATED;
			if (_cache_logging_in_critical_section) {
				srfsLog(LOG_FINE, "Creating op for missing cache entry %llx", key);
			}
			op = createOp(createOpContext, key, minModificationTimeMicros);
			if (_cache_logging_in_critical_section) {
				srfsLog(LOG_FINE, "Storing op in cache %llx", key);
			}
			// create internal ref for cache, ref deleted internally when not active op anymore
			aor = aor_new(op, __FILE__, __LINE__);
			newEntry = cache_entry_new(CACHE_ACTIVE_OP, key, keySize, aor, 
                            sizeof(ActiveOpRef *), CACHE_NO_MODIFICATION_TIME,
                            newOpTimeoutMillis);
			cache_store_entry(cache, newEntry, TRUE);
			// create external ref, deleted externally
			*activeOpRef = aor_new(op, __FILE__, __LINE__);
			if (_cache_logging_in_critical_section) {
				srfsLog(LOG_FINE, "Created ActiveOpRef %llx", *activeOpRef);
			}
		} else {
			result = CRR_NOT_FOUND;
			if (cacheNumRead != NULL) {
				*cacheNumRead = 0;
			}
			//if (createOpContext != NULL) {
			//	fatalError("createOpContext != NULL", __FILE__, __LINE__);
			//}
		}
    }
	pthread_rwlock_unlock(&cache->rwLock);
	if (!speculativeRead) {
		pthread_spin_lock(&cache->statLock);
		cache->stats.readResults[result]++;
		pthread_spin_unlock(&cache->statLock);
	} else {
		pthread_spin_lock(&cache->statLock);
		cache->stats.specReadResults[result]++;
		pthread_spin_unlock(&cache->statLock);
	}
    return result;
}

static CacheEntry *cache_evict(Cache *cache, CacheEntry *entry) {
	CacheEntry	*removedEntry;
	void		*key;
	
	key = entry->key;
	removedEntry = (CacheEntry *)hashtable_remove(cache->ht, key);
	if (removedEntry != entry) {
		fatalError("removedEntry != entry", __FILE__, __LINE__);
	}
	if (removedEntry != NULL) {
		cache_entry_delete(&removedEntry, TRUE, __FILE__, __LINE__);
	} else {
		fatalError("NULL entry", __FILE__, __LINE__);
	}
	return removedEntry;
}

static int cache_evict_list(Cache *cache, LRUList *lruList) {
	int	i;
	
	for (i = 0; i < lruList->curEntries; i++) {
		cache_evict(cache, lruList->lruEntries[i]);
	}
	return lruList->curEntries;
}

static void cache_add_eviction_candidate(LRUList *lruList, CacheEntry *entry) {
	// simple linear algorithm for now
	int	i;

	i = 0;
	while (i < lruList->curEntries && entry->lastAccess < lruList->lruEntries[i]->lastAccess) {
		i++;
	}
	if (lruList->curEntries < lruList->sizeLimit) {
		int	j;
		
		for (j = lruList->sizeLimit - 1; j >= i + 1; j--) {
			lruList->lruEntries[j] = lruList->lruEntries[j - 1];
		}
		lruList->lruEntries[i] = entry;
		lruList->curEntries++;
	} else if (i > 0) {
		int	j;
		
		for (j = 0; j < i - 1; j++) {
			lruList->lruEntries[j] = lruList->lruEntries[j + 1];
		}
		lruList->lruEntries[i - 1] = entry;
	}
}

// must hold lock when calling 
static void cache_evict_if_needed(Cache *cache) {
    struct hashtable_itr *itr;
	int	searchComplete;
	int	numToEvict;
	int	numFailedEvictions;

    if (_cache_logging_in_critical_section) {
        srfsLog(LOG_WARNING, "cache_evict_if_needed %llx cache->size %d\n", cache, cache->size);
    }
    // simplistic operation - simply remove first entry
	numToEvict = 0;
	numFailedEvictions = 0;
    if (cache->size == CACHE_UNLIMITED_SIZE) {
		numToEvict = 0;
	} else {
        int ht_count;
		/*
		if (hashtable_count(cache->ht) < (unsigned int)cache->size) {
			// cache isn't full, no need to evict
			numToEvict = 0;
		} else {
			numToEvict = hashtable_count(cache->ht) - (unsigned int)cache->size;
			if (numToEvict < cache->evictionBatchSize) {
				numToEvict = cache->evictionBatchSize;
			}
		}
		*/
        ht_count = hashtable_count(cache->ht);
		numToEvict = ht_count - (unsigned int)cache->size;
		if (_cache_logging_in_critical_section) {
            srfsLog(LOG_WARNING, "cache_evict_if_needed %llx numToEvict %d ht_count %d cache->size %d\n", 
                              cache, numToEvict, ht_count, cache->size);
        }
		if (numToEvict <= 0) {
			// cache isn't full, no need to evict
			numToEvict = 0;
		} else {
			if (numToEvict < cache->evictionBatchSize) {
				numToEvict = cache->evictionBatchSize;
			}
		}
    }
	if (_cache_logging_in_critical_section) {
        srfsLog(LOG_WARNING, "cache_evict_if_needed %llx numToEvict %d\n", cache, numToEvict);
    }
	if (numToEvict > 0) {
		LRUList		lruList;
		CacheEntry	*lruEntries[numToEvict];
		int			numEvicted;
        uint64_t    _curTimeMillis;
		
		lruList.lruEntries = lruEntries;
		lruList.sizeLimit = numToEvict;
		lruList.curEntries = 0;
		memset(lruEntries, 0, numToEvict * sizeof(CacheEntry *));
		if (_cache_logging_in_critical_section) {
			srfsLog(LOG_FINE, "cache_evict_if_needed %d", numToEvict);
		}
		itr = hashtable_iterator(cache->ht);
		if (itr == NULL) {
			fatalError("NULL itr", __FILE__, __LINE__);
		}
        
        _curTimeMillis = curTimeMillis();
		//hashtable_iterator_debug(itr);
		searchComplete = FALSE;
		while (!searchComplete) {
			int			notAtEnd;
			CacheEntry	*entry;
			void    	*key;

			key = hashtable_iterator_key(itr);
			if (key == NULL) {
				fatalError("NULL key", __FILE__, __LINE__);
			}
			//entry = (CacheEntry *)hashtable_search(cache->ht, key);
            entry = (CacheEntry *)hashtable_iterator_value(itr);
			if (_cache_logging_in_critical_section) {
                srfsLog(LOG_FINE, "key %llx entry %llx", key, entry);
            }
			if (entry == NULL) {
                srfsLog(LOG_ERROR, "key %llx entry %llx", key, entry);
				fatalError("NULL entry", __FILE__, __LINE__);
			}
			notAtEnd = hashtable_iterator_advance(itr);
			if (_cache_logging_in_critical_section) {
				srfsLog(LOG_FINE, "entry->type %d ", entry->type);
			}
			if (entry->type != CACHE_ACTIVE_OP
                    || _curTimeMillis > entry->expirationTime) {
				cache_add_eviction_candidate(&lruList, entry);
			} else {
				if (_cache_debug_failed_evictions) {
					srfsLog(LOG_WARNING, "fe entry->type %d ", entry->type);
				}
				numFailedEvictions++;
			}
			if (!notAtEnd) {
				searchComplete = TRUE;
			}
		}
		free(itr);	
		itr = NULL;
		
		numEvicted = cache_evict_list(cache, &lruList);
		
		if (numEvicted > 0) {
			pthread_spin_lock(&cache->statLock);
			cache->stats.evictions += numEvicted;
			pthread_spin_unlock(&cache->statLock);
		}
		if (numFailedEvictions > 0) {
			pthread_spin_lock(&cache->statLock);
			cache->stats.failed_evictions += numFailedEvictions;
			pthread_spin_unlock(&cache->statLock);
		}
	}
}

static int cache_data_type_sanity_check(CacheEntry *entry, CacheReplacementMode crm) {
	if (!cache_entry_is_data_type(entry)) {
		srfsLog(LOG_ERROR, "Entry: %llx %d %lu", entry, entry->type, entry->size);
        if (crm == CRM_FATAL_ERROR) {
            fatalError("Entry is not a data type", __FILE__, __LINE__);
        } else {
            srfsLog(LOG_ERROR, "Entry is not a data type %s %d", __FILE__, __LINE__);
        }
        return FALSE;
	} else {
        return TRUE;
    }
}

static void cache_replacement_sanity_check(CacheEntry *e1, CacheEntry *e2, 
                                           CacheReplacementMode crm) {
    int t1OK;
    int t2OK;    
    
	t1OK = cache_data_type_sanity_check(e1, crm);
	t2OK = cache_data_type_sanity_check(e2, crm);
	if ((t1OK && t2OK) && cache_entry_get_data_size(e1) != cache_entry_get_data_size(e2)) {
		srfsLog(LOG_ERROR, "cache_entry_get_data_size(e1) != cache_entry_get_data_size(e2)");
	}
}

static CacheStoreResult cache_store_entry(Cache *cache, CacheEntry *entry, int alreadyLocked, int notifyActiveOps_noStorage, CacheReplacementMode crm) {
	CacheEntry	*oldEntry;
	CacheStoreResult	result;

	if (_cache_logging_in_critical_section) {
		srfsLog(LOG_FINE, "in cacheStore %s %x\n", cache->name, entry);
	}
	if (!alreadyLocked) {
		pthread_rwlock_wrlock(&cache->rwLock);
	}
	//pthread_spin_lock(&cache->statLock);
	cache->stats.writes++;
	//pthread_spin_unlock(&cache->statLock);
	//oldEntry = (CacheEntry *)hashtable_remove(cache->ht, entry->key);
	// can't remove here since we might need to leave it in and
	// the hashtable destroys keys upon removal
    oldEntry = (CacheEntry *)hashtable_search(cache->ht, entry->key); 
	if (oldEntry != NULL) {
		if (entry->type == CACHE_ACTIVE_OP) {
			// active op creation is governed by an external lock that
			// guarantees only one ActiveOp created at a time for
			// a given key
			srfsLog(LOG_ERROR, "more than one active op for key %llx", entry->key);
			fatalError("More than one ActiveOp for a key");
		}
		
		if (_cache_logging_in_critical_section) {
			srfsLog(LOG_FINE, "attempting to replace %llx with %llx", oldEntry, entry);
		}
		switch (oldEntry->type) {
		case CACHE_ACTIVE_OP:
			if (_cache_logging_in_critical_section) {
				srfsLog(LOG_FINE, "replacing active op %llx with %llx", oldEntry, entry);
			}
			// release the cache reference on the active op
			aor_delete((ActiveOpRef **)&oldEntry->data);
			break;
		case CACHE_RAW_DATA: // fall through to next case
		case CACHE_DHT_VALUE:
			if (crm == CRM_ALLOW) {
				if (_cache_logging_in_critical_section) {
					srfsLog(LOG_FINE, "allowing replacement %llx in place of %llx", entry, oldEntry);
				}
				// Allow replacement
			} else {
				// No replacement allowed
				cache_replacement_sanity_check(oldEntry, entry, crm);
				// after sanity check, ignore the new entry
				// by deleting the attempted addition and reinserting the oldEntry
				if (_cache_logging_in_critical_section) {
					srfsLog(LOG_FINE, "ignoring replacement %llx and keeping %llx", entry, oldEntry);
				}
				// the caller is notified of rejection
				// caller is responsible for deleting data
				cache_entry_delete(&entry, FALSE, __FILE__, __LINE__);
				entry = oldEntry;
				if (_cache_logging_in_critical_section) {
					srfsLog(LOG_FINE, "oldEntry %llx oldEntry->key %llx", oldEntry, oldEntry->key);
				}
			}
			break;
		case CACHE_ERROR_CODE:
			switch (entry->type) {
			//case CACHE_RAW_DATA: 
			//	break;
			//case CACHE_DHT_VALUE: 
			//	break;
			case CACHE_ERROR_CODE:
				if (memcmp(oldEntry->data, entry->data, sizeof(int))) {
                    // previously, we did not allow replacement
                    // now we do, no action required
					//fatalError("Mismatching error codes in replacement attempt", __FILE__, __LINE__);
				} else {
					cache_entry_delete(&entry, TRUE, __FILE__, __LINE__);
					entry = oldEntry;
				}
				break;
			default: 
                // previously, we did not allow replacement
                // now we do, no action required
                //fatalError("Unexpected entry->type for CACHE_ERROR_CODE replacement", __FILE__, __LINE__);
                srfsLog(LOG_FINE, "%s %d", __FILE__, __LINE__);
			}
			if (_cache_logging_in_critical_section) {
				srfsLog(LOG_FINE, "replacing error code %llx with %llx", oldEntry, entry);
			}
			break;
		default: fatalError("Unexpected oldEntry->type", __FILE__, __LINE__);
		}
		if (entry != oldEntry) {
			CacheEntry	*removedEntry;
			
			if (_cache_logging_in_critical_section) {
				srfsLog(LOG_FINE, "in entry != oldEntry");
			}
			removedEntry = (CacheEntry *)hashtable_remove(cache->ht, entry->key);
			if (removedEntry != oldEntry) {
				fatalError("removedEntry != oldEntry", __FILE__, __LINE__);
			} else {
				cache_entry_delete(&oldEntry, TRUE, __FILE__, __LINE__);
				// Above was commented out, but if we leave it out, the old entries will leak.
				// This will cause the data to be deleted. 
			}
			// insert the replacement entry
			if (!notifyActiveOps_noStorage) {
				hashtable_insert(cache->ht, entry->key, entry); 
				result = CACHE_STORE_SUCCESS;
			} else {
				result = CACHE_STORE_OLD_REMOVED_NOTHING_STORED;
				cache_entry_delete(&entry, TRUE, __FILE__, __LINE__);
			}
		} else {
			result = CACHE_STORE_ALREADY_PRESENT;
		}
	} else {
		cache_evict_if_needed(cache);
		hashtable_insert(cache->ht, entry->key, entry); 
		result = CACHE_STORE_SUCCESS;
	}
	//value = hashtable_search(cache->ht, key);
	//srfsLog(LOG_WARNING, "sanity check %llx", value);
	if (!alreadyLocked) {
	    pthread_rwlock_unlock(&cache->rwLock);
	}
	if (_cache_logging_in_critical_section) {
		srfsLog(LOG_FINE, "out cacheStore %s %x\n", cache->name);
	}
	return result;
}

CacheStoreResult cache_store_dht_value(Cache *cache, void *key, int keySize, SKVal *pRVal, 
                                        uint64_t modificationTimeMicros, uint64_t timeoutMillis) {
	return cache_store_entry(cache, cache_entry_new(CACHE_DHT_VALUE, key, keySize, pRVal, sizeof(SKVal *), modificationTimeMicros, timeoutMillis));
}

CacheStoreResult cache_store_raw_data(Cache *cache, void *key, int keySize, void *data, size_t length, int replace, uint64_t modificationTimeMicros, uint64_t timeoutMillis) {
	return cache_store_entry(cache, cache_entry_new(CACHE_RAW_DATA, key, keySize, data, length, 
                modificationTimeMicros, timeoutMillis), FALSE, FALSE, replace ? CRM_ALLOW : CRM_FATAL_ERROR);
}

void cache_store_active_op(Cache *cache, void *key, int keySize, ActiveOp *op) {
	fatalError("deprecated", __FILE__, __LINE__);
	(void) cache; (void) key; (void) keySize; (void) op; //fix for "unused parameter" warning
	/*
	CacheStoreResult	result;
	
	result = cache_store_entry(cache, cache_entry_new(CACHE_ACTIVE_OP, key, keySize, op, sizeof(ActiveOp *), TRUE));
	if (result != CACHE_STORE_SUCCESS) {
		fatalError("result != CACHE_STORE_SUCCESS", __FILE__, __LINE__);
	}
	*/
}

void cache_store_error(Cache *cache, void *key, int keySize, int errorCode, 
                int notifyActiveOps_noStorage, uint64_t modificationTimeMicros, uint64_t timeoutMillis) {
	int	*_errorCode;

	_errorCode = int_dup(&errorCode);
	srfsLog(LOG_FINE, "storing in cache error %d", errorCode);
	cache_store_entry(cache, cache_entry_new(CACHE_ERROR_CODE, key, keySize, 
        _errorCode, sizeof(int), modificationTimeMicros, timeoutMillis), FALSE,
        notifyActiveOps_noStorage, CRM_WARN);
}

static CacheEntry	*_cache_remove(Cache *cache, void *key, int removeActiveOps) {
	CacheEntry	*entry;
    int         removed;

    pthread_rwlock_wrlock(&cache->rwLock);
	if (!removeActiveOps) {
		entry = (CacheEntry *)hashtable_search(cache->ht, key); 
        if (entry != NULL && entry->type != CACHE_ACTIVE_OP) {
            entry = (CacheEntry *)hashtable_remove(cache->ht, key);
            removed = TRUE;
        } else {
            removed = FALSE;
            entry = NULL;
        }
    } else {
        entry = (CacheEntry *)hashtable_remove(cache->ht, key);
        removed = TRUE;
    }    
    pthread_rwlock_unlock(&cache->rwLock);
    if (removed) {
        pthread_spin_lock(&cache->statLock);
        cache->stats.removals++;
        pthread_spin_unlock(&cache->statLock);
    }
	return entry;
}

void cache_remove(Cache *cache, void *key, int removeActiveOps) {
	CacheEntry	*entry;

	entry = _cache_remove(cache, key, removeActiveOps);
	if (entry != NULL) {
		cache_entry_delete(&entry);
	} else {
		srfsLog(LOG_INFO, "Attempted to delete non-existent entry %s %s", 
                            cache->name, removeActiveOps ? "or active op" : "");
	}	
}

void cache_remove_active_op(Cache *cache, void *key, int fatalErrorOnNotFound) {
	CacheEntry	*entry;

	entry = _cache_remove(cache, key);
	if (entry != NULL) {
		if (entry->type != CACHE_ACTIVE_OP) {
            if (fatalErrorOnNotFound) {
                fatalError("Not an active op", __FILE__, __LINE__);
            } else {
                srfsLog(LOG_WARNING, "%s not an active op", cache->name, __FILE__, __LINE__);
            }
		} else {
			if (entry->data == NULL) {
				fatalError("entry->data == NULL", __FILE__, __LINE__);
			} else {
				ActiveOpRef	*aor;
				
				aor = (ActiveOpRef *)entry->data;
				aor_delete(&aor);
			}
		}
	} else {
        if (fatalErrorOnNotFound) {
            fatalError("Can't find active op for removal", __FILE__, __LINE__);
        } else {
            srfsLog(LOG_WARNING, "%s can't find active op for removal", cache->name, __FILE__, __LINE__);
        }
	}	
}

// cache entry

static CacheEntry *cache_entry_new(CacheEntryType type, void *key, size_t keySize, void *data, size_t size, uint64_t modificationTime, uint64_t timeoutMillis) {
	CacheEntry	*entry;
	uint64_t	_curTimeMillis;

	entry = (CacheEntry *)mem_alloc(1, sizeof(CacheEntry));
	entry->key = mem_alloc_no_dbg(keySize, 1);
	memcpy(entry->key, key, keySize);
	entry->type = type;
	entry->data = data;
	entry->size = size;
    entry->modificationTime = modificationTime;
    // FUTURE - make the below clock user a lighter-weight clock
    // to avoid heavyweight calls while holding the cache lock
	_curTimeMillis = curTimeMillis();
	if (timeoutMillis != CACHE_NO_TIMEOUT) {
		entry->expirationTime = _curTimeMillis + timeoutMillis;
	} else {
		entry->expirationTime = _cache_no_expiration;
	}
	entry->lastAccess = _curTimeMillis;
	return entry;
}

static int cache_entry_is_data_type(CacheEntry *entry) {
	return entry->type == CACHE_RAW_DATA || entry->type == CACHE_DHT_VALUE;
}

static size_t cache_entry_get_data_size(CacheEntry *entry) {
		switch (entry->type) {
		case CACHE_RAW_DATA:
			return entry->size;
		case CACHE_DHT_VALUE:
			return ((SKVal *)entry->data)->m_len;
		default:
			fatalError("detected invalid CacheEntry", __FILE__, __LINE__);
			return 0;
		}
}

static void cache_entry_delete(CacheEntry **entry, int deleteData, char *file, int line) {
	if (entry != NULL && *entry != NULL) {
		if (_cache_logging_in_critical_section) {
			srfsLog(LOG_FINE, "cache_entry_delete %llx %llx %s %d", 
                    *entry, (*entry)->key, file, line);
		}
		//mem_free(&(*entry)->key); hashtable_remove clears out the keys so no need for this
		if (deleteData) {
			switch ((*entry)->type) {
			case CACHE_RAW_DATA:
				mem_free(&(*entry)->data);
				break;
			case CACHE_DELETED_ENTRY:
                if (_cache_fatal_error_on_double_free) {
                    fatalError("Attempted to free CACHE_DELETED_ENTRY", file, line);
                } else {
                    srfsLog(LOG_WARNING, "Attempted to free CACHE_DELETED_ENTRY");
                    return;
                }
                break;
			case CACHE_DHT_VALUE:
                {
                SKVal   *pVal;
                
                pVal = (SKVal *)(*entry)->data;
                if (pVal != NULL) {
                    if (pVal->m_rc != SKOperationState::SUCCEEDED 
                            && pVal->m_rc != SKOperationState::INCOMPLETE 
                            && pVal->m_rc != SKOperationState::FAILED) {
                        if (_cache_fatal_error_on_double_free) {
                            fatalError("Bad m_rc in SKVal free. Possible double free", file, line);
                        } else {
                            srfsLog(LOG_WARNING, "Bad m_rc for SKVal free in cache_entry_delete. Possible double free");
                            return;
                        }
                    }
                }
				sk_destroy_val((SKVal **)&(*entry)->data);
                }
				break;
			case CACHE_ACTIVE_OP:
				if ((*entry)->data != NULL) {
                    if (curTimeMillis() < (*entry)->expirationTime) {
                        fatalError("attempted to free non-NULL, non-expired ActiveOp");
                    }
				}
				break;
			case CACHE_ERROR_CODE:
				mem_free(&(*entry)->data);
				break;
			default:
				fatalError("detected invalid CacheEntry", __FILE__, __LINE__);
				break;
			}
		}
        (*entry)->type = CACHE_DELETED_ENTRY;
		mem_free((void **)entry, __FILE__, __LINE__);
	} else {
		fatalError("bad ptr in cache_entry_delete");
	}
}

// presumes NULL-terminated keys
CacheKeyList cache_key_list(Cache *cache) {
    struct hashtable_itr *itr;
	int		walkComplete;
	int		entries;
	int		keyIndex;
	CacheKeyList	keyList;
	
	memset(&keyList, 0, sizeof(CacheKeyList));
    pthread_rwlock_rdlock(&cache->rwLock);
	{
		entries = hashtable_count(cache->ht);
		keyList.size = entries;
		if (entries == 0) {
			keyList.keys = NULL;
			pthread_rwlock_unlock(&cache->rwLock);
			return keyList;
		}
		keyList.keys = (char **)mem_alloc(entries, sizeof(char *));
		itr = hashtable_iterator(cache->ht);
		if (itr == NULL) {
			fatalError("NULL itr", __FILE__, __LINE__);
		}
		keyIndex = 0;
		walkComplete = FALSE;
		while (!walkComplete) {
			int			notAtEnd;
			void    	*key;

			key = hashtable_iterator_key(itr);
			if (key == NULL) {
				fatalError("NULL key", __FILE__, __LINE__);
			}
			keyList.keys[keyIndex] = str_dup((char *)key);
			notAtEnd = hashtable_iterator_advance(itr);
			if (!notAtEnd) {
				walkComplete = TRUE;
			}
		}
		free(itr);	
		itr = NULL;
	}
	pthread_rwlock_unlock(&cache->rwLock);
	return keyList;
}

static void cache_display(Cache *cache) {
    struct hashtable_itr *itr;
	int	walkComplete;
	int	entries;
	
	srfsLog(LOG_WARNING, "\ncache %s", cache->name);
	entries = hashtable_count(cache->ht);
	if (entries == 0) {
		return;
	}
	itr = hashtable_iterator(cache->ht);
	if (itr == NULL) {
		fatalError("NULL itr", __FILE__, __LINE__);
	}
	//hashtable_iterator_debug(itr);
	/*
	if (hashtable_iterator_key(itr) == NULL) {
		walkComplete = TRUE;
	} else {
		walkComplete = FALSE;
	}
	*/
	walkComplete = FALSE;
	while (!walkComplete) {
		int			notAtEnd;
		CacheEntry	*entry;
		void    	*key;

		key = hashtable_iterator_key(itr);
		if (key == NULL) {
			fatalError("NULL key", __FILE__, __LINE__);
		}
		entry = (CacheEntry *)hashtable_search(cache->ht, key);
		srfsLog(LOG_WARNING, "key %llx entry %llx", key, entry);
		if (entry == NULL) {
			fatalError("NULL entry", __FILE__, __LINE__);
		}
		srfsLog(LOG_WARNING, "entry->type %d", 
                    entry->type);
		notAtEnd = hashtable_iterator_advance(itr);
		if (!notAtEnd) {
			walkComplete = TRUE;
		}
	}
	free(itr);	
	itr = NULL;
}


void cache_display_stats(Cache *cache) {
	int	i;
	int	entries;

	pthread_rwlock_rdlock(&cache->rwLock);
	entries = hashtable_count(cache->ht);
	pthread_rwlock_unlock(&cache->rwLock);
	srfsLog(LOG_WARNING, "n: %s\tsize: %d\tentries: %d", cache->name, cache->size, entries);

	for (i = CRR_NOT_FOUND; i < CRR_CODE_TRAILER - 1; i++) {
		srfsLog(LOG_WARNING, "%s:%llu", crr_strings[i], cache->stats.readResults[i]);
	}
	i = CRR_FOUND;
	srfsLog(LOG_WARNING, "spec:%s:%llu", crr_strings[i], cache->stats.specReadResults[i]);
	i = CRR_ACTIVE_OP_CREATED;
	srfsLog(LOG_WARNING, "spec:%s:%llu", crr_strings[i], cache->stats.specReadResults[i]);

	srfsLog(LOG_WARNING, "e: %llu", cache->stats.evictions);
	srfsLog(LOG_WARNING, "fe: %llu", cache->stats.failed_evictions);
	srfsLog(LOG_WARNING, "r: %llu", cache->stats.removals);
	
	//if (entries > cache->size + 10) {
	//	cache_display(cache); // for debugging only
	//}
}
