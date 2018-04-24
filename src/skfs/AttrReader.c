// AttrReader.c

/////////////
// includes

#include "ActiveOp.h"
#include "ActiveOpRef.h"
#include "AttrReadRequest.h"
#include "AttrReader.h"
#include "FileID.h"
#include "G2OutputDir.h"
#include "G2TaskOutputReader.h"
#include "OpenDirTable.h"
#include "SRFSConstants.h"
#include "Util.h"

#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <set>
#include <map>
#include <vector>
#include <string>

////////////
// defines

#define SNAPSHOT_STRING ".snapshot"
#define CUR_LINK_STRING "cur"
#define _AR_MAX_LSTAT_RETRIES   8
#define _AR_LSTAT_RETRY_SLEEP_SECONDS   1
#define _AR_ERR_TIMEOUT_MILLIS (10 * 1000)


///////////////////////
// private prototypes

static void ar_process_native_request(void *_requestOp, int curThreadIndex);
static void ar_process_dht_batch(void **requests, int numRequests, int curThreadIndex);
static void ar_process_prefetch(void **requests, int numRequests, int curThreadIndex);
static void ar_parse_native_alias(AttrReader *ar, char *mapping);
static void ar_store_native_alias_attribs(AttrReader *ar);
static void ar_translate_reverse_path(AttrReader *ar, char *path, const char *nativePath);
static int ar_is_no_error_cache_path(AttrReader *ar, char *path);
static void _ar_store_dir_attribs(AttrReader *ar, char *path, uint16_t mode);
static int _ar_get_attr(AttrReader *ar, char *path, FileAttr *fa, int isNativePath, char *nativePath, uint64_t minModificationTimeMicros);
static void ar_store_attr_in_cache(AttrReadRequest *arr, FileAttr *fa, uint64_t timeoutMillis = CACHE_NO_TIMEOUT);
static void ar_process_prefetch(void **requests, int numRequests, int curThreadIndex);

/////////////////
// private data

static FileAttr		_attr_does_not_exist;
static AttrReader	*_global_ar; // FUTURE - allow for multiple

// FUTURE - AttrReader contains nfs alias mapping code that should move elsewhere in the future


///////////////////
// implementation

AttrReader *ar_new(FileIDToPathMap *f2p, SRFSDHT *sd, AttrWriter *aw,
				   ResponseTimeStats *rtsDHT, ResponseTimeStats *rtsNFS, int numSubCaches,
                   uint64_t attrTimeoutMillis) {
	AttrReader *ar;

	ar = (AttrReader*)mem_alloc(1, sizeof(AttrReader));
	ar->attrCache = ac_new(numSubCaches);
    ar->attrTimeoutMillis = attrTimeoutMillis;
	ar->nfsAttrQueueProcessor = qp_new(ar_process_native_request, __FILE__, __LINE__, AR_NFS_QUEUE_SIZE, ABQ_FULL_BLOCK, AR_NFS_THREADS);
	ar->dhtAttrQueueProcessor = qp_new_batch_processor(ar_process_dht_batch, __FILE__, __LINE__, 
								AR_DHT_QUEUE_SIZE, ABQ_FULL_DROP, AR_DHT_THREADS, AR_MAX_BATCH_SIZE);
	ar->attrPrefetchProcessor = qp_new_batch_processor(ar_process_prefetch, __FILE__, __LINE__, 
								AR_PREFETCH_QUEUE_SIZE, ABQ_FULL_DROP, AR_PREFETCH_THREADS, AR_PREFETCH_MAX_BATCH_SIZE);
	ar->f2p = f2p;
	ar->aw = aw;
	ar->sd = sd;
	ar->rtsDHT = rtsDHT;
	ar->rtsNFS = rtsNFS;
	ar->rs = rs_new();
	ar->noErrorCachePaths = pg_new("noErrorCachePaths");
	ar->noLinkCachePaths = pg_new("noLinkCachePaths", FALSE);
	ar->snapshotOnlyPaths = pg_new("snapshotOnlyPaths", FALSE);
	try {
		int	i;
		
		for (i = 0; i < AR_DHT_THREADS; i++) {
			SKNamespace	*ns;
			SKNamespacePerspectiveOptions *nspOptions;
			
			ar->pSession[i] = sd_new_session(ar->sd);
			ns = ar->pSession[i]->getNamespace(SKFS_ATTR_NS);
			nspOptions = ns->getDefaultNSPOptions();
			ar->ansp[i] = ns->openAsyncPerspective(nspOptions);
			delete ns;
		}
	} catch(SKClientException & ex){
		srfsLog(LOG_ERROR, "ar_new exception opening namespace %s: what: %s\n", SKFS_ATTR_NS, ex.what());
		fatalError("exception in ar_new", __FILE__, __LINE__ );
	}

	_global_ar = ar; // FUTURE - allow for multiple
	
	return ar;
}

void ar_delete(AttrReader **ar) {
	if (ar != NULL && *ar != NULL) {
		(*ar)->dhtAttrQueueProcessor->running = FALSE;
		(*ar)->nfsAttrQueueProcessor->running = FALSE;
		ac_delete(&(*ar)->attrCache);
		for(int i=0; i<(*ar)->dhtAttrQueueProcessor->numThreads; i++) {
			int added = qp_add((*ar)->dhtAttrQueueProcessor, NULL);
			if (!added) srfsLog(LOG_ERROR, "ar_delete failed to add NULL to dhtAttrQueueProcessor\n");
		}
		for(int i=0; i<(*ar)->nfsAttrQueueProcessor->numThreads; i++) {
			int added = qp_add((*ar)->nfsAttrQueueProcessor, NULL);
			if (!added) srfsLog(LOG_ERROR, "ar_delete failed to add NULL to nfsAttrQueueProcessor\n");
		}
		qp_delete(&(*ar)->dhtAttrQueueProcessor);
		qp_delete(&(*ar)->nfsAttrQueueProcessor);
		try {
			int	i;
			
			for (i = 0; i < AR_DHT_THREADS; i++) {
				(*ar)->ansp[i]->waitForActiveOps();
				(*ar)->ansp[i]->close();
				delete (*ar)->ansp[i];
			}
		} catch (SKRetrievalException & e ){
			srfsLog(LOG_ERROR, "fbr dht batch at %s:%d\n%s\n", __FILE__, __LINE__, e.what());
			srfsLog(LOG_ERROR, " %s\n",  e.getDetailedFailureMessage().c_str());
			fatalError("exception in ar_delete", __FILE__, __LINE__ );
		} catch (std::exception & ex) {
			srfsLog(LOG_ERROR, "exception in ar_delete: what: %s\n", ex.what());
			fatalError("exception in ar_delete", __FILE__, __LINE__ );
		}
		rs_delete(&(*ar)->rs);
		pg_delete(&(*ar)->noErrorCachePaths);
		pg_delete(&(*ar)->noLinkCachePaths);
		pg_delete(&(*ar)->snapshotOnlyPaths);
		
		if ((*ar)->pSession) {
			int	i;
			
			for (i = 0; i < AR_DHT_THREADS; i++) {
				if ((*ar)->pSession[i]) {
					delete (*ar)->pSession[i];
					(*ar)->pSession[i] = NULL;
				}
			}
		}
		
		mem_free((void **)ar);
	} else {
		fatalError("bad ptr in ar_delete");
	}
}

void ar_set_g2tor(AttrReader *ar, G2TaskOutputReader *g2tor) {
	ar->g2tor = g2tor;
}

// paths

static int ar_is_no_error_cache_path(AttrReader *ar, char *path) {
	return pg_matches(ar->noErrorCachePaths, path) || is_writable_path(path);
}

void ar_parse_no_error_cache_paths(AttrReader *ar, char *paths) {
	pg_parse_paths(ar->noErrorCachePaths, paths);
}

int ar_is_no_link_cache_path(AttrReader *ar, char *path) {
	return pg_matches(ar->noLinkCachePaths, path);
}

void ar_parse_no_link_cache_paths(AttrReader *ar, char *paths) {
	pg_parse_paths(ar->noLinkCachePaths, paths);
}

static int ar_is_snapshot_only_path(AttrReader *ar, char *path) {
	srfsLog(LOG_FINE, "ar_is_snapshot_only_path %s", path);
	return pg_matches(ar->snapshotOnlyPaths, path);
}

void ar_parse_snapshot_only_paths(AttrReader *ar, char *paths) {
	pg_parse_paths(ar->snapshotOnlyPaths, paths);
}

AttrCache *ar_get_attrCache(AttrReader *ar) {
	return ar->attrCache;
}

void ar_ensure_path_fid_associated(AttrReader *ar, char * path, FileID *fid) {
    if (f2p_get(ar->f2p, fid) == NULL) {
        f2p_put(ar->f2p, (FileID *)mem_dup_no_dbg(fid, sizeof(FileID)), path); 
    }
}

// cache

static void ar_store_attr_in_cache(AttrReadRequest *arr, FileAttr *fa, uint64_t timeoutMillis) {
	FileAttr	*cachedFileAttr;
	CacheStoreResult	result;

	srfsLog(LOG_FINE, "storing in cache %s", arr->path);
	cachedFileAttr = (FileAttr *)mem_dup(fa, sizeof(FileAttr));
	// below works for permanent, but not for writable which may change
	//f2p_put(arr->attrReader->f2p, &cachedFileAttr->fid, arr->path); 
	// below allows attr cache to purge attr w/o affecting f2p
	f2p_put(arr->attrReader->f2p, (FileID *)mem_dup_no_dbg(&cachedFileAttr->fid, sizeof(FileID)), arr->path); 
	// store f2p before cache to ensure that anything that can read the cache
	// can get the mapping
	result = ac_store_raw_data(arr->attrReader->attrCache, arr->path, cachedFileAttr, TRUE, arr->minModificationTimeMicros, timeoutMillis);
	if (result == CACHE_STORE_SUCCESS) {
		srfsLog(LOG_FINE, "storing in f2p %s", arr->path);
		//f2p_put(arr->attrReader->f2p, fid_new_native(cachedStat), arr->path); // moved to above
	} else {
		srfsLog(LOG_FINE, "Cache store rejected.");
        if (cachedFileAttr != NULL) {
		    mem_free((void **)&cachedFileAttr);
        }
	}
}

static void ar_process_dht_batch(void **requests, int numRequests, int curThreadIndex) {
	SKOperationState::SKOperationState   dhtMgetErr;
	AttrReader		*ar;
	int				i;
	int				j;
	ActiveOpRef		*refs[numRequests]; // Convenience cast of requests to ActiveOpRef*
	uint64_t		t1;
	uint64_t		t2;
    SKAsyncValueRetrieval *pValRetrieval;
    StrValMap       *pValues;
    int             isDuplicate[numRequests];

	srfsLog(LOG_FINE, "in ar_process_dht_batch %d", curThreadIndex);
	ar = NULL;
    StrVector       requestGroup;  // sets of keys
    
    memset(isDuplicate, 0, sizeof(int) * numRequests);
	
	// First create a group of keys to request
	for (int i = 0; i < numRequests; i++) {
		ActiveOp		*op;
		AttrReadRequest	*arr;

		refs[i] = (ActiveOpRef *)requests[i];
		op = refs[i]->ao;
		arr = (AttrReadRequest *)ao_get_target(op);
		arr_display(arr, LOG_FINE);
		if (ar == NULL) {
			ar = arr->attrReader;
		} else {
			if (ar != arr->attrReader) {
				fatalError("multi AttrReader batch");
			}
		}
		srfsLog(LOG_FINE, "looking in dht for attrib %s", arr->path);
		requestGroup.push_back(arr->path);
	}

	// Now fetch the batch from the KVS
	srfsLog(LOG_FINE, "ar_process_dht_batch calling multi_get");
    pValRetrieval = NULL;
    pValues = NULL;
    srfsLog(LOG_INFO, "got async nsp %s ", SKFS_ATTR_NS );
    try {
	    t1 = curTimeMillis();
	    pValRetrieval = ar->ansp[curThreadIndex]->get(&requestGroup);
        pValRetrieval->waitForCompletion();
        t2 = curTimeMillis();
        rts_add_sample(ar->rtsDHT, t2 - t1, numRequests);
        dhtMgetErr = pValRetrieval->getState();
        srfsLog(LOG_FINE, "ar_process_dht_batch multi_get complete %d", dhtMgetErr);
        pValues = pValRetrieval->getValues();
    } catch (SKRetrievalException & e) {
		// The operation generated an exception. This is typically simply because
		// values were not found for one or more keys.
		// This could also, however, be caused by a true error.
		dhtMgetErr = SKOperationState::FAILED;
        //srfsLog(LOG_WARNING, e.getStackTrace().c_str() );
		
		// Go through the original requests and obtain the results
			// FUTURE: probably should improve the efficiency of this approach.
			// Can turn off the exception for not found.
			
			// Also, this function contains a large amount of duplicative code for
			// the exception vs. non-exception case
			// We should be able to make one pass over the values for either case
			
        for (int i = 0; i < numRequests; ++i){
            ActiveOp		*op;
            AttrReadRequest	*arr;
            int				successful;
            int             errorCode;
			
            errorCode = 0;
            successful = FALSE;
            op = refs[i]->ao;
            arr = (AttrReadRequest *)ao_get_target(op);
            SKOperationState::SKOperationState opState = e.getOperationState(arr->path);

            if (opState == SKOperationState::SUCCEEDED) {
                SKStoredValue   *pStoredVal;
                
                // these are successfully retrieved keys
                pStoredVal = NULL; 
				pStoredVal = e.getStoredValue(arr->path);
				if (!pStoredVal) {
					srfsLog(LOG_WARNING, "ar dhtErr no pStoredVal %s %d %d", arr->path, opState, __LINE__);
				} else {
					SKVal   *pval;
                    
					pval = NULL;
					pval = pStoredVal->getValue();
					if (!pval){
						srfsLog(LOG_WARNING, "ar dhtErr no val %s %d %d", arr->path, opState, __LINE__);
					} else {
						if (pval->m_len == sizeof(FileAttr) ){
                            uint64_t	attrTimeoutMillis;
                        
                            attrTimeoutMillis = is_writable_path(arr->path) ? ar->attrTimeoutMillis : CACHE_NO_TIMEOUT;
							if (memcmp(pval->m_pVal, &_attr_does_not_exist, sizeof(FileAttr))) {

								// a normal data item, store in cache
                                ao_set_complete(op, AOResult_Success, pval->m_pVal, sizeof(FileAttr));
								ar_store_attr_in_cache(arr, (FileAttr *)pval->m_pVal, attrTimeoutMillis);
								successful = TRUE;
							} else {
								if (SRFS_ENABLE_DHT_ENOENT_CACHING) {
									// non-existence, store as an error
									srfsLog(LOG_FINE, "%s not found in DHT. Storing ENOENT. %s %d", arr->path, __FILE__, __LINE__);
                                    ao_set_complete_error(op, ENOENT);
									ac_store_error(arr->attrReader->attrCache, arr->path, ENOENT, arr->minModificationTimeMicros, attrTimeoutMillis);
									successful = TRUE;
								} else {
									srfsLog(LOG_FINE, "!SRFS_ENABLE_DHT_ENOENT_CACHING. Ignoring ENOENT found in DHT.");
								}
							}
						} else {
							srfsLog(LOG_WARNING, "val->size() != sizeof(FileAttr) %s %d", __FILE__, __LINE__);
							pval = NULL; // temp workaround the problem - FUTURE improve
						}
					}
					if (pval != NULL) {
                        // No need to check for duplicates here as this value
                        // is created from SKRetrievalException on each iteration
						sk_destroy_val(&pval);
					}
					delete pStoredVal;
				}
            } else if (opState == SKOperationState::INCOMPLETE){
                sd_op_failed(ar->sd, opState, __FILE__, __LINE__);
	            srfsLog(LOG_FINE, "%s not found in dht", arr->path);
				srfsLog(LOG_WARNING, "SKRetrievalException at %s:%d\n%s\n", __FILE__, __LINE__, e.what());
				srfsLog(LOG_WARNING, " %s\n",  e.getDetailedFailureMessage().c_str());
                errorCode = EIO;
            } else /*(opState == SKOperationState::FAILED) */ {
				// these keys are failed with cause
				SKFailureCause::SKFailureCause cause = SKFailureCause::ERROR;
				try {
					cause = e.getFailureCause(arr->path);
				} catch(SKClientException & e2) { 
					srfsLog(LOG_WARNING, "ar dhtErr getFailureCause at %s:%d\n%s\n", __FILE__, __LINE__, e2.what()); 
					sd_op_failed(ar->sd, opState, __FILE__, __LINE__);
				} catch(std::exception& e2) { 
					//should this be fatal error?
					srfsLog(LOG_WARNING, "ar dhtErr getFailureCause at %s:%d\n%s\n", __FILE__, __LINE__, e2.what()); 
					sd_op_failed(ar->sd, opState, __FILE__, __LINE__);
				}
				
				if (cause == SKFailureCause::NO_SUCH_VALUE) { 
					srfsLog(LOG_FINE, "ar dhtErr %s %d %d %d/%d line %d", arr->path, opState, cause, i, numRequests, __LINE__);
                    errorCode = ENOENT;
				} else {
					srfsLog(LOG_WARNING, "SKRetrievalException at %s:%d\n%s\n", __FILE__, __LINE__, e.what());
					srfsLog(LOG_WARNING, " %s\n",  e.getDetailedFailureMessage().c_str());
					sd_op_failed(ar->sd, opState, __FILE__, __LINE__);
					srfsLog(LOG_WARNING, "ar dhtErr %s %d %d %d/%d line %d", arr->path, opState, cause, i, numRequests, __LINE__);
                    errorCode = EIO;
				}
            }

            if (successful) {
                // set complete above; no need to set complete here
            } else {
				if (!is_writable_path(arr->path)) {
					srfsLog(LOG_FINE, "native. set op stage dht+1 %llx", op);
					ao_set_stage(op, SRFS_OP_STAGE_DHT + 1);
				} else {
					srfsLog(LOG_FINE, "skfs. set complete %llx", op);
                    if (errorCode == 0) {
                        errorCode = ENOENT;
                    }
                    ac_remove_active_op(arr->attrReader->attrCache, arr->path);
                    ao_set_complete_error(op, errorCode);
                    //ac_store_error(arr->attrReader->attrCache, arr->path, errorCode, ar->attrTimeoutMillis);
				}
            }
            aor_delete(&refs[i]);
		}

		pValRetrieval->close();
		delete pValRetrieval;
		srfsLog(LOG_FINE, "out ar_process_dht_batch");
		return;
	} catch (SKClientException & e) {
        srfsLog(LOG_WARNING, "ar line %d SKClientException %s\n", __LINE__, e.what());
		dhtMgetErr = SKOperationState::FAILED;
		// Shouldn't reach here as the only currently thrown exception 
		// is a RetrievalException which is handled above
		fatalError("ar unexpected SKClientException", __FILE__, __LINE__);
	}

    if (!pValues){
        srfsLog(LOG_WARNING, "ar dhtErr no keys from namespace %s", SKFS_ATTR_NS);
        sd_op_failed(ar->sd, dhtMgetErr, __FILE__, __LINE__);
        for (i = 0; i < numRequests; i++) {
            ActiveOp		*op;
            
            op = refs[i]->ao;
            ao_set_complete_error(op, EIO);
        }
    } else {
        OpStateMap  *opStateMap;
        
        // Check for duplicates
        if (pValues->size() != numRequests) {
            // Naive n^2 search for the duplicates that must exist
            for (i = 0; i < numRequests; i++) {
                for (j = i + 1; j < numRequests; j++) {
                    AttrReadRequest    *arr_i = (AttrReadRequest *)ao_get_target(refs[i]->ao);
                    AttrReadRequest    *arr_j = (AttrReadRequest *)ao_get_target(refs[j]->ao);

                    if (!strcmp(arr_i->path, arr_j->path)) {
                        isDuplicate[j] = TRUE;
                    }
                }
            }
        }
        
        opStateMap = pValRetrieval->getOperationStateMap();
        for (i = 0; i < numRequests; i++) {
            if (!isDuplicate[i]) {
                ActiveOp		*op;
                AttrReadRequest	*arr;
                int				successful;
                SKVal           *ppval;
                SKOperationState::SKOperationState opState;
                int             errorCode;
                
                errorCode = 0;
                successful = FALSE;
                op = refs[i]->ao;
                arr = (AttrReadRequest *)ao_get_target(op);
                try {
                    ppval = pValues->at(arr->path);
                } catch(std::exception& emap) { 
                    ppval = NULL;
                    srfsLog(LOG_INFO, "ar std::map exception at %s:%d\n%s\n", __FILE__, __LINE__, emap.what()); 
                }
                try {
                    opState = opStateMap->at(arr->path);
                } catch(std::exception& emap) { 
                    opState = SKOperationState::FAILED;
                    srfsLog(LOG_INFO, "ar std::map exception at %s:%d\n%s\n", __FILE__, __LINE__, emap.what()); 
                }
                if (opState == SKOperationState::SUCCEEDED) {
                    if (ppval == NULL ){
                        srfsLog(LOG_FINE, "ar dhtErr no val %s %d line %d", arr->path, opState,  __LINE__);
                    } else if (ppval->m_len == sizeof(FileAttr) ){
                        uint64_t	attrTimeoutMillis;
                    
                        attrTimeoutMillis = is_writable_path(arr->path) ? ar->attrTimeoutMillis : CACHE_NO_TIMEOUT;
                        if (memcmp(ppval->m_pVal, &_attr_does_not_exist, sizeof(FileAttr))) {
                            // a normal data item, store in cache
                            ao_set_complete(op, AOResult_Success, ppval->m_pVal, sizeof(FileAttr));
                            ar_store_attr_in_cache(arr, (FileAttr *)ppval->m_pVal, attrTimeoutMillis);
                            successful = TRUE;
                        } else {
                            if (SRFS_ENABLE_DHT_ENOENT_CACHING) {
                                // non-existence, store as an error
                                srfsLog(LOG_FINE, "%s not found in DHT. Storing ENOENT. %s %d", arr->path, __FILE__, __LINE__);
                                ao_set_complete_error(op, ENOENT);
                                ac_store_error(arr->attrReader->attrCache, arr->path, ENOENT, arr->minModificationTimeMicros, attrTimeoutMillis);
                                successful = TRUE;
                            } else {
                                srfsLog(LOG_FINE, "!SRFS_ENABLE_DHT_ENOENT_CACHING. Ignoring ENOENT found in DHT.");
                            }
                        }
                    } else {
                        srfsLog(LOG_WARNING, "val->size() != sizeof(FileAttr) %s %d", __FILE__, __LINE__);
                        ppval = NULL; // temp workaround the problem - FUTURE - improve
                    }
                } else if (opState == SKOperationState::INCOMPLETE) {
                    sd_op_failed(ar->sd, dhtMgetErr, __FILE__, __LINE__);
                    srfsLog(LOG_WARNING, "%s not found in kvs. Incomplete operation state.", arr->path);
                    errorCode = EIO;
                } else {  //SKOperationState::FAILED
                    SKFailureCause::SKFailureCause cause = SKFailureCause::ERROR;
                    try {
                        cause = pValRetrieval->getFailureCause();
                        if (cause == SKFailureCause::MULTIPLE) {
                            errorCode = EIO;
                            sd_op_failed(ar->sd, dhtMgetErr, __FILE__, __LINE__);
                            srfsLog(LOG_WARNING, "ar dhtErr %s %d %d %d/%d line %d", arr->path, opState, cause, i, numRequests, __LINE__);
                        } else if (cause != SKFailureCause::NO_SUCH_VALUE) {
                            sd_op_failed(ar->sd, dhtMgetErr, __FILE__, __LINE__);
                            srfsLog(LOG_WARNING, "ar dhtErr %s %d %d %d/%d line %d", arr->path, opState, cause, i, numRequests, __LINE__);
                            errorCode = EIO;
                        } else { 
                            srfsLog(LOG_FINE, "ar dhtErr %s %d %d %d/%d line %d", arr->path, opState, cause, i, numRequests, __LINE__);
                            errorCode = ENOENT;
                        }
                    } catch (SKClientException &e) { 
                        srfsLog(LOG_ERROR, "ar getFailureCause at %s:%d\n%s\n", __FILE__, __LINE__, e.what()); 
                        sd_op_failed(ar->sd, dhtMgetErr, __FILE__, __LINE__);
                    } catch (std::exception &e) { 
                        srfsLog(LOG_ERROR, "ar getFailureCause exception at %s:%d\n%s\n", __FILE__, __LINE__, e.what()); 
                        sd_op_failed(ar->sd, dhtMgetErr, __FILE__, __LINE__);
                    }
                    // for r/o paths, errors are stored permanently also
                    // for errors other than ENOENT, the calling code will retry to the file system
                    //ac_store_error(arr->attrReader->attrCache, arr->path, -1);
                    // FIXME - for skfs files, no result will cause the operation to hang
                }
                if (ppval != NULL) {
                    sk_destroy_val( &ppval );
                }

                if (successful) {
                    // set complete above; no need to set complete here
                } else {
                    if (!is_writable_path(arr->path)) {
                        srfsLog(LOG_FINE, "set op stage dht+1 %llx", op);
                        ao_set_stage(op, SRFS_OP_STAGE_DHT + 1);
                    } else {
                        srfsLog(LOG_FINE, "set op complete %llx %s %d", op, __FILE__, __LINE__);
                        if (errorCode == 0) {
                            errorCode = ENOENT;
                        }
                        
                        ac_remove_active_op(arr->attrReader->attrCache, arr->path);
                        ao_set_complete_error(op, errorCode);
                    }
                }
            } else {
                // No duplicate-specific action required
            }
            aor_delete(&refs[i]);
        }
        delete opStateMap; 
        delete pValues;
    }

    pValRetrieval->close();
    delete pValRetrieval;
	srfsLog(LOG_FINE, "out ar_process_dht_batch");
}

static void ar_process_native_request(void *_requestOpRef, int curThreadIndex) {
	ActiveOpRef	*ref;
	ActiveOp	*op;
	AttrReadRequest	*arr;
	int		res;
	struct stat	tmpStat;
	char	*nativePath;
	uint64_t	t1;
	uint64_t	t2;
	int			allowRead;
	int			errnoValue;

	srfsLog(LOG_FINE, "in ar_process_native_request %d %llx", curThreadIndex, _requestOpRef);
	ref = (ActiveOpRef *)_requestOpRef;
	op = ref->ao;
	arr = (AttrReadRequest *)ao_get_target(op);
	arr_display(arr, LOG_FINE);
	nativePath = arr->path; // we require translation to have already taken place
	srfsLog(LOG_FINE, "lstat %s", nativePath);
	/*
	if (!ar_is_snapshot_only_path(arr->attrReader, nativePath)) {
		allowRead = TRUE;
		srfsLog(LOG_FINE, "not snapshot only path");
	} else {
		allowRead = (strstr(nativePath, SNAPSHOT_STRING) != NULL) 
				 || (strstr(nativePath, CUR_LINK_STRING) != NULL);
		srfsLog(LOG_FINE, "snapshot only result %d %d %d", allowRead, 
			strstr(nativePath, SNAPSHOT_STRING) != NULL, strstr(nativePath, CUR_LINK_STRING) != NULL);
	}
	*/
	allowRead = TRUE;
	if (allowRead) {
		t1 = curTimeMillis();
		res = lstat(nativePath, &tmpStat);
		t2 = curTimeMillis();
		rts_add_sample(arr->attrReader->rtsNFS, t2 - t1, 1);
		errnoValue = errno;
	} else {
		res = -1;
		errnoValue = ENOENT;
	}
	if (res == 0) {
		FileAttr	*fa;
		
		tmpStat.st_blksize = SRFS_BLOCK_SIZE;
        tmpStat.st_nlink = FA_NATIVE_LINK_MAGIC;
		fa = fa_new_native(&tmpStat);
        ao_set_complete(op, AOResult_Success, fa, sizeof(FileAttr));
		ar_store_attr_in_cache(arr, fa);
		srfsLog(LOG_FINE, "sending to AttrWriter %s inode %lu", arr->path, tmpStat.st_ino);
		fa_delete(&fa);
        tmpStat.st_nlink = FA_NATIVE_LINK_NORMAL;
		fa = fa_new_native(&tmpStat);
		aw_write_attr(arr->attrReader->aw, arr->path, fa);
		fa_delete(&fa);
	} else {
		// if not found, store in DHT unless it's a no error cache path
		if (errnoValue == ENOENT) {
            // for r/o paths, ENOENT is stored permanently
            // for errors other than ENOENT, the calling code will retry to the file system
            ao_set_complete_error(op, errnoValue);
            ac_store_error(arr->attrReader->attrCache, arr->path, errnoValue, arr->minModificationTimeMicros);
			if (!ar_is_no_error_cache_path(arr->attrReader, nativePath)) {
				if (SRFS_ENABLE_DHT_ENOENT_CACHING) {
					srfsLog(LOG_FINE, "storing _attr_does_not_exist to DHT for %s", nativePath);
					aw_write_attr(arr->attrReader->aw, arr->path, &_attr_does_not_exist);
				}
			} else {
				srfsLog(LOG_FINE, "ENOENT. not storing due to no error cache path %s", nativePath);
			}
		} else {
			srfsLog(LOG_WARNING, "lstat error %s %d", nativePath, errnoValue);
            // Non ENOENT errors are stored temporarily
            // For errors other than ENOENT, the calling code will also retry to the file system
            ao_set_complete_error(op, errnoValue);
            ac_store_error(arr->attrReader->attrCache, arr->path, errnoValue, arr->minModificationTimeMicros, _AR_ERR_TIMEOUT_MILLIS);
		}
	}
	srfsLog(LOG_FINE, "set op complete %llx", _requestOpRef);
    
    // set complete above; no need to set complete here
    
	// we do not need to destroy the request since it is an ActiveOp and will 
	// be destroyed externally
	aor_delete(&ref);
	srfsLog(LOG_FINE, "out ar_process_native_request %llx", _requestOpRef);
}


// native alias

static int ar_is_native_alias(AttrReader *ar, char *path) {
	int	i;

    for (i = 0; i < ar->numNFSAliases; i++) {
		if (!strcmp(ar->nfsLocalAliases[i], path)) {
			return TRUE;
		}
    }
	return FALSE;
}

static void ar_parse_native_alias(AttrReader *ar, char *mapping) {
    char        *s;

    //printf("parseNFSAlias %s\n", mapping);
    s = strchr(mapping, ':');
    if (s != NULL) {
        strncpy(ar->nfsLocalAliases[ar->numNFSAliases], mapping, s - mapping);
        strcpy(ar->nfsRoots[ar->numNFSAliases], s + 1);
        srfsLog(LOG_WARNING, "NFSMapping %s -> %s\n", ar->nfsLocalAliases[ar->numNFSAliases], ar->nfsRoots[ar->numNFSAliases]);
        ar->numNFSAliases++;
    }
}

void ar_parse_native_aliases(AttrReader *ar, char *nfsMapping) {
    char        *cur;
    char        *div;
    char        *next;

    ar->numNFSAliases = 0;
    cur = (char *)nfsMapping;
    while (cur != NULL) {
        div = strchr(cur, ',');
        if (div == NULL) {
            next = NULL;
        } else {
            next = div + 1;
            *div = '\0';
        }
        ar_parse_native_alias(ar, cur);
        cur = next;
    }
	ar_store_native_alias_attribs(ar);
}

static void _ar_store_dir_attribs(AttrReader *ar, char *path, uint16_t mode) {
	FileAttr	*fa;
	struct stat	st;

	memset(&st, 0, sizeof(struct stat));
	st.st_mode = S_IFDIR | mode;
	st.st_nlink = 2;
	st.st_atime = BIRTHDAY_3;
	st.st_mtime = BIRTHDAY_2;
	st.st_ctime = BIRTHDAY_1;
	st.st_uid = get_uid();
	st.st_gid = get_gid();
	st.st_blksize = SRFS_BLOCK_SIZE;
	fa = fa_new(fid_generate_new_skfs_internal(), &st);
	ac_store_raw_data(ar->attrCache, path, fa, FALSE); // FIXME - verify result
}

static void ar_store_native_alias_attribs(AttrReader *ar) {
    int	i;

    for (i = 0; i < ar->numNFSAliases; i++) {
		ar_store_dir_attribs(ar, ar->nfsLocalAliases[i]);
    }
}

void ar_store_dir_attribs(AttrReader *ar, char *path, uint16_t mode) {
	strcpy(ar->externalDirs[ar->numExternalDirs], path);
	ar->numExternalDirs++;
	_ar_store_dir_attribs(ar, path, mode);
}

void ar_translate_path(AttrReader *ar, char *nativePath, const char *path) {
    int	i;

    for (i = 0; i < ar->numNFSAliases; i++) {
		int aliasLength;

		aliasLength = strlen(ar->nfsLocalAliases[i]);
		if (!strncmp(path, ar->nfsLocalAliases[i], aliasLength)) {
			if (path[aliasLength] == '/' || path[aliasLength] == '\0') {
				sprintf(nativePath, "%s%s", ar->nfsRoots[i], &path[aliasLength]); 
				srfsLog(LOG_FINE, "nativePath %s\n", nativePath);
				return;
			}
		}
    }
	srfsLog(LOG_FINE, "nativePath using path %s\n", path);
    strcpy(nativePath, path);
}

static void ar_translate_reverse_path(AttrReader *ar, char *path, const char *nativePath) {
    int	i;

    for (i = 0; i < ar->numNFSAliases; i++) {
		int rootLength;

		rootLength = strlen(ar->nfsRoots[i]);
		if (!strncmp(nativePath, ar->nfsRoots[i], rootLength)) {
			sprintf(path, "%s%s", ar->nfsLocalAliases[i], &path[rootLength]); 
			srfsLog(LOG_FINE, "revnativePath %s\n", nativePath);
			return;
		}
    }
	srfsLog(LOG_FINE, "path using nfsPpath %s\n", nativePath);
    strcpy(path, nativePath);
}

static int ar_is_valid_path(AttrReader *ar, const char *path) {
	if (!strcmp(path, "/")) {
		return TRUE;
	} else {
		int	i;

		for (i = 0; i < ar->numExternalDirs; i++) {
			int dirLength;

			dirLength = strlen(ar->externalDirs[i]);
			if (!strncmp(path, ar->externalDirs[i], dirLength)) {
				return TRUE;
			}
		}
		return FALSE;
	}
}

void ar_create_alias_dirs(AttrReader *ar, OpenDirTable *odt) {
	int	i;

    srfsLog(LOG_INFO, "ar_create_alias_dirs");
    for (i = 0; i < ar->numNFSAliases; i++) {
        srfsLog(LOG_INFO, "create alis dir %s", ar->nfsLocalAliases[i]);
        if (odt_add_entry(odt, SKFS_BASE, (ar->nfsLocalAliases[i] + 1))) { // +1 is to skip the leading slash, which entries do not contain
            srfsLog(LOG_WARNING, "Couldn't create new entry in parent for %s", ar->nfsLocalAliases[i]);
            fatalError("Couldn't create dir", __FILE__, __LINE__);
        }
    }
}


// Callback from Cache. Cache write lock is held during callback.
ActiveOp *ar_create_active_op(void *_ar, void *_nativePath, uint64_t minModificationTimeMicros) {
	AttrReader *ar;
	char *nativePath;
	ActiveOp *op;
	AttrReadRequest	*attrReadRequest;

	ar = (AttrReader *)_ar;
	nativePath = (char *)_nativePath;
	srfsLog(LOG_FINE, "ar_create_active_op %s", nativePath);
	attrReadRequest = arr_new(ar, nativePath, minModificationTimeMicros);
	arr_display(attrReadRequest, LOG_FINE);
	op = ao_new(attrReadRequest, (void (*)(void **))arr_delete);
	return op;
}

int ar_get_attr_stat(AttrReader *ar, char *path, struct stat *st) {
	FileAttr	fa;
	int			result;
	
	result = ar_get_attr(ar, path, &fa);
	if (result == 0) {
        if (!fa_is_deleted_file(&fa)) {
            if (S_ISLNK(fa.stat.st_mode) && is_base_path(path) && !is_writable_path(path)) {
                char    nativePath[SRFS_MAX_PATH_LENGTH];
                char    resolvedPath[SRFS_MAX_PATH_LENGTH];
                int     pathLength;
    
                // For base paths that are symlinks, we return the attribute
                // of the *target*. This avoids being bounced out to the native fs
                // for base directories that are links (e.g. for NFS mount points.)
                memset(nativePath, 0, SRFS_MAX_PATH_LENGTH);
                memset(resolvedPath, 0, SRFS_MAX_PATH_LENGTH);    
                ar_translate_path(ar, nativePath, path);                
                pathLength = readlink(nativePath, resolvedPath, SRFS_MAX_PATH_LENGTH - 1);
                if (pathLength == -1) {
                    result == -1;
                } else {
                    // First stat the original path in case we need to induce an automount
                    result = lstat(nativePath, st);
                    if (result == -1) {
                        srfsLog(LOG_ERROR, "stat failed at %s %d", __FILE__, __LINE__);
                    }
                    srfsLog(LOG_FINE, "Reading base path %s from symlink %s target %s",
                            path, nativePath, resolvedPath);
                    // Now read from the target
                    result = lstat(resolvedPath, st);
                }
            } else {
                memcpy(st, &fa.stat, sizeof(struct stat));
            }
        } else {
            return ENOENT;
        }
	}
	return result;
}

int ar_get_attr(AttrReader *ar, char *path, FileAttr *fa, uint64_t minModificationTimeMicros) {
	int isValidPath;
	int isNativePath;
	int errorCode;
	char    nativePath[SRFS_MAX_PATH_LENGTH];

	srfsLog(LOG_FINE, "in ar_get_attr: %s", path);

	// First validate paths
	
	isValidPath = ar_is_valid_path(ar, path);
	srfsLog(LOG_FINE, "isValidPath: %s %d", path, isValidPath);
	if (!isValidPath) {
		srfsLog(LOG_WARNING, "invalid path %s", path);
		return ENOENT;
	}

	if (ar_is_snapshot_only_path(ar, path)) {
		int	allowRead;

		allowRead = (strstr(path, SNAPSHOT_STRING) != NULL) 
				 || (strstr(path, CUR_LINK_STRING) != NULL);
		srfsLog(LOG_FINE, "snapshot only result %d %d %d", allowRead, 
			strstr(path, SNAPSHOT_STRING) != NULL, strstr(path, CUR_LINK_STRING) != NULL);
		if (!allowRead) {
			return ENOENT;
		}
	}
	
	isNativePath = !is_writable_path(path);
	srfsLog(LOG_FINE, "isNativePath %s", isNativePath ? "true" : "false");
	
	// Translate the SKFS path into a native path
	ar_translate_path(ar, nativePath, path);
	srfsLog(LOG_FINE, "translated %s --> %s", path, nativePath);	    
    
	errorCode = _ar_get_attr(ar, path, fa, isNativePath, nativePath, minModificationTimeMicros);
    // If error code is non-zero, fa will contain the error code.
    // We must not treat fa as a valid FileAttr in this case.
	
	if (errorCode != 0) {
		int	res;
        int originalErrorCode;
		
        originalErrorCode = errorCode;
		// Found an error. Handle it			
		if (errorCode != 0) {
            // First clear fa so that we don't accidentally treat it
            // as a valid FileAttr when it's just carrying the error code.
			memset(fa, 0, sizeof(FileAttr));
		}
		if (errorCode != ENOENT) {
			// Error is a true error
			if (isNativePath) {
				struct stat	stbuf;
                int _retries;
				
				// For native paths, try a last ditch native operation (with retries)
                _retries = 0;
				memset(&stbuf, 0, sizeof(struct stat));
                do {
                    res = lstat(nativePath, &stbuf);
                    if (res != 0) {
                        errorCode = errno;
                        if (errorCode != ENOENT) {
                            sleep(_AR_LSTAT_RETRY_SLEEP_SECONDS);
                        }
                    }
                } while (res != 0 && errorCode != ENOENT && ++_retries < _AR_MAX_LSTAT_RETRIES);
				if (res == 0) {
					errorCode = 0;
					fa_init_native(fa, &stbuf);
                    memcpy(&fa->stat, &stbuf, sizeof(struct stat));
					f2p_put(ar->f2p, fid_new_native(&stbuf), nativePath); 
				} else {
					errorCode = errno;
				}
			} else {
				// leave errorCode unchanged
			}
			srfsLog(LOG_WARNING, "path %s nativePath %s errorCode %d originalErrorCode %d line %d\n",
                                path, nativePath, errorCode, originalErrorCode, __LINE__);
		} else {
			// Error is ENOENT
			if (ar_is_no_error_cache_path(ar, nativePath)) {
				if (isNativePath) {
					struct stat	stbuf;
					
					// This path has error caching turned off. Consult the native FS.
					srfsLog(LOG_FINE, "is no_error_cache_path %s", nativePath);
					memset(&stbuf, 0, sizeof(struct stat));
					res = lstat(nativePath, &stbuf);
					if (res == 0) {
						errorCode = 0;
						fa_init_native(fa, &stbuf);
						f2p_put(ar->f2p, fid_new_native(&stbuf), nativePath); 
					} else {
						errorCode = errno;
					}
					srfsLog(LOG_FINE, "non-error caching path %s rechecked res %d errorCode %d", nativePath, res, errorCode);
				} else {
					// leave errorCode unchanged
				}
			} else {
				// Found ENOENT. Caching is active. Return ENOENT.
				srfsLog(LOG_FINE, "not no_error_cache_path %s", nativePath);
			}
		}
	}
	/*
	if (1) {
		char	dbg[4096];
		
		memset(dbg, 0, 4096);
		fa_to_string(fa, dbg, 4096);
		srfsLog(LOG_WARNING, "ar_get_attr fa: %s", dbg);
		stat_display(&fa->stat, stderr);
	}
	*/
	return errorCode;
}

static void ar_cp_rVal_to_fa(FileAttr *fa, ActiveOpRef *aor, char *file, int line) {
    size_t  rValLength;
    
    rValLength = aor_get_rValLength(aor);
    if (rValLength != sizeof(FileAttr)) {
            srfsLog(LOG_ERROR, "Unexpected rValLength %u %u %s %d", 
                    rValLength, sizeof(FileAttr), file, line); 
    }
    memcpy(fa, aor_get_rVal(aor), rValLength);
}

static int _ar_get_attr(AttrReader *ar, char *path, FileAttr *fa, int isNativePath, char *nativePath, uint64_t minModificationTimeMicros) {
	CacheReadResult	result;
    AOResult        aoResult;
	ActiveOpRef		*activeOpRef;
	int				returnCode;
	
    aoResult = AOResult_Incomplete;
	returnCode = 0;
	activeOpRef = NULL;
	
	// Look in the AttrCache for an existing operation
	// Create a new operation if none exists

	srfsLog(LOG_FINE, "looking in cache for %s", nativePath);
	result = ac_read(ar->attrCache, nativePath, fa, &activeOpRef, ar, minModificationTimeMicros);
	srfsLog(LOG_FINE, "cache result %d %s", result, crr_strings[result]);
	if (result == CRR_FOUND && !memcmp(fa, &_attr_does_not_exist, sizeof(FileAttr))) {
		if (SRFS_ENABLE_DHT_ENOENT_CACHING) {
			srfsLog(LOG_FINE, "_attr_does_not_exist found. Setting result to ENOENT");
		} else {
			srfsLog(LOG_WARNING, "Unexpected _attr_does_not_exist found in cache. Setting result to ENOENT");
		}
		result = CRR_ERROR_CODE;
		*((int *)fa) = ENOENT;
        rs_cache_inc(ar->rs);
	}
	if (result != CRR_ACTIVE_OP_CREATED) {
		// No operation was created. We must have a result, an existing operation, or an error code to return.
		if (result == CRR_FOUND) {
			// Found in cache, and hence already copied out to stbuf by ac_read()
			// We only need to update cache stats here
			//srfsLog(LOG_FINE, "ar_get_attr inode %lu \n", stbuf->st_ino );
			srfsLog(LOG_FINE, "out ar_get_attr: %s\tfound in cache %u", nativePath, fa->stat.st_size);
			rs_cache_inc(ar->rs);
			return 0;
		} else if (result == CRR_ACTIVE_OP_EXISTING) {
			int	errorCode;
            
            // Found an existing operation.
			// Copy the data out (done below at end)
			// No fallback to native here since the initial operation will do that
			srfsLog(LOG_FINE, "waiting for existing op completion %s %llx", nativePath, activeOpRef->ao);
			aoResult = aor_wait_for_completion(activeOpRef);
            switch (aoResult) {
            case AOResult_Incomplete:
                fatalError("Unexpected AOResult_Incomplete", __FILE__, __LINE__);
                break;
            case AOResult_Timeout:
                errorCode = EIO;
                break;
            case AOResult_Error:
                errorCode = (int)(uint64_t)aor_get_rVal(activeOpRef);
                break;
            case AOResult_Success:
                ar_cp_rVal_to_fa(fa, activeOpRef, __FILE__, __LINE__);
                errorCode = 0;
                break;
            default:
                fatalError("panic", __FILE__, __LINE__);
            }
			aor_delete(&activeOpRef);
			rs_opWait_inc(ar->rs);
            
            return errorCode;
		} else if (result == CRR_ERROR_CODE) {
			int	errorCode;
			
			errorCode = *((int *)fa);
			return errorCode;
		} else {
			// Can't legitimately reach here. All cases should have been handled above.
			fatalError("panic", __FILE__, __LINE__);
            return -1;
		}
	} else {
		ActiveOp *op;
        int     errorCode;

		// A new operation was created. 
		
		// First look in the dht
		srfsLog(LOG_FINE, "CRR_ACTIVE_OP_CREATED. Adding to dht queue %s", nativePath);
		op = activeOpRef->ao;

		if (!isNativePath || sd_is_enabled(ar->sd)) {
			uint64_t	timeout;
			int			added;
			ActiveOpRef	*aor;

			srfsLog(LOG_FINE, "Queueing op %s %llx", nativePath, op);
			aor = aor_new(op, __FILE__, __LINE__);
			added = qp_add(ar->dhtAttrQueueProcessor, aor);
			if (!added) {
				aor_delete(&aor);
			}
			if (isNativePath) {
				timeout = sd_get_dht_timeout(ar->sd, ar->rtsDHT, ar->rtsNFS, 1);
				srfsLog(LOG_FINE, "waiting for op dht stage completion %s %u", nativePath, timeout);
				aoResult = aor_wait_for_stage_timed(activeOpRef, SRFS_OP_STAGE_DHT, timeout);
                
                if (aoResult != AOResult_Success) {
                    // For native, also look in native fs simultaneously
                    rs_nfs_inc(ar->rs);
                    srfsLog(LOG_FINE, "Queueing nfs op %s %llx", nativePath, op);
                    added = qp_add(ar->nfsAttrQueueProcessor, aor_new(op, __FILE__, __LINE__));
                    if (!added) {
                        // nfsAttrQueueProcessor blocks if full, so addition will not fail
                        fatalError("panic", __FILE__, __LINE__);
                    }
                    srfsLog(LOG_FINE, "waiting for nfs op completion %s %llx", nativePath, activeOpRef->ao);
                    aoResult = aor_wait_for_completion(activeOpRef);
                } else {
                    rs_dht_inc(ar->rs);
                }                
 			} else {
				srfsLog(LOG_FINE, "Not-native. Waiting for dht op completion %s", nativePath);
				aoResult = aor_wait_for_stage_timed(activeOpRef, SRFS_OP_STAGE_DHT, FBR_DHT_STAGE_WRITABLE_FS_TIMEOUT_MS);
                if (aoResult == AOResult_Success) {
                    rs_dht_inc(ar->rs);
                }
			}
			srfsLog(LOG_FINE, "op dht stage complete %s", nativePath);
            
            switch (aoResult) {
            case AOResult_Incomplete:
                fatalError("Unexpected AOResult_Incomplete", __FILE__, __LINE__);
                break;
            case AOResult_Timeout:
                errorCode = ETIMEDOUT;
                break;
            case AOResult_Error:
                errorCode = (int)(uint64_t)aor_get_rVal(activeOpRef);
                break;
            case AOResult_Success:
                ar_cp_rVal_to_fa(fa, activeOpRef, __FILE__, __LINE__);
                errorCode = 0;
                break;
            default:
                fatalError("panic", __FILE__, __LINE__);
            }
		} else {
			srfsLog(LOG_FINE, "sd not enabled. skipping dht");
            ac_remove_active_op(ar->attrCache, nativePath);
            errorCode = EAGAIN;
            ao_set_complete_error(op, errorCode);
		}
        return errorCode;
	}
    fatalError("panic", __FILE__, __LINE__); // unreachable
    return -1;
}

CacheStoreResult ar_store_attr_in_cache_static(char *path, FileAttr *fa, int replace, uint64_t modificationTimeMicros, uint64_t timeoutMillis) {
	AttrReader		*ar;
	FileAttr	*cachedFileAttr;
	CacheStoreResult	result;
	ar = _global_ar; // FUTURE - allow for multiple

	srfsLog(LOG_FINE, "storing in cache %s", path);
	cachedFileAttr = (FileAttr *)mem_dup(fa, sizeof(FileAttr));
    
	// below works for permanent, but not for writable which may change
	//f2p_put(arr->attrReader->f2p, &cachedFileAttr->fid, arr->path); 
	srfsLog(LOG_FINE, "storing in f2p %s", path);
	// below allows attr cache to purge attr w/o affecting f2p
	f2p_put(ar->f2p, (FileID *)mem_dup_no_dbg(&cachedFileAttr->fid, sizeof(FileID)), path); 
	// store f2p before cache to ensure that anything that can read the cache
	// can get the mapping
	result = ac_store_raw_data(ar->attrCache, path, cachedFileAttr, replace, modificationTimeMicros, timeoutMillis);
	if (result != CACHE_STORE_SUCCESS) {
		srfsLog(LOG_FINE, "Cache store rejected.");
        if (cachedFileAttr != NULL) {
		    mem_free((void **)&cachedFileAttr);
        }
	}
	return result;
}

static void ar_process_prefetch(void **requests, int numRequests, int curThreadIndex) {
	SKOperationState::SKOperationState   dhtMgetErr;
	AttrReader		*ar;
	int				i;
	char			*paths[numRequests]; // Convenience cast of paths to char*
	uint64_t		t1;
	uint64_t		t2;

	srfsLog(LOG_FINE, "in ar_process_prefetch %d", curThreadIndex);
	ar = _global_ar; // FUTURE - allow for multiple
	
	// First create a group of keys to request
    StrVector       requestGroup;  // sets of keys
	for (int i = 0; i < numRequests; i++) {
		char			*path;

		paths[i] = (char *)requests[i];
		path = paths[i];
		srfsLog(LOG_FINE, "prefetching attrib %s", path);
		requestGroup.push_back(path);
	}

	// Now fetch the batch from the KVS
	srfsLog(LOG_FINE, "ar_process_prefetch calling multi_get");
    SKAsyncValueRetrieval *pValRetrieval = NULL;
    StrValMap *pValues = NULL;
    srfsLog(LOG_INFO, "got async nsp %s ", SKFS_ATTR_NS);
    try {
	    t1 = curTimeMillis();
	    pValRetrieval = ar->ansp[curThreadIndex]->get(&requestGroup);
        pValRetrieval->waitForCompletion();
        t2 = curTimeMillis();
        //rts_add_sample(ar->rtsDHT, t2 - t1, numRequests);
        dhtMgetErr = pValRetrieval->getState();
        srfsLog(LOG_FINE, "ar_process_prefetch multi_get complete %d", dhtMgetErr);
        pValues = pValRetrieval->getValues();
    } catch (SKRetrievalException & e) {
		// The operation generated an exception. This is typically simply because
		// values were not found for one or more keys.
		// This could also, however, be caused by a true error.
		dhtMgetErr = SKOperationState::FAILED;
        //srfsLog(LOG_WARNING, e.getStackTrace().c_str() );
		
		// Go through the original requests and obtain the results
			// FUTURE: probably should improve the efficiency of this approach.
			// Can turn off the exception for not found.
			
			// Also, this function contains a large amount of duplicative code for
			// the exception vs. non-exception case
			// We should be able to make one pass over the values for either case
			
        for (int i = 0; i < numRequests; ++i){
            int		successful;
			char	*path;
			
            successful = FALSE;
            path = paths[i];
            SKOperationState::SKOperationState opState = e.getOperationState(path);

			srfsLog(LOG_FINE, "a: prefetch %s %d", path, opState);
            if (opState == SKOperationState::SUCCEEDED) {
                // these are successfully retrieved keys
                SKStoredValue *pStoredVal = NULL; 
				pStoredVal = e.getStoredValue(path);
				if (!pStoredVal) {
					srfsLog(LOG_WARNING, "ar dhtErr no pStoredVal %s %d %d", path, opState, __LINE__);
				} else {
					SKVal *pval = NULL;
					pval = pStoredVal->getValue();
					if (!pval){
						srfsLog(LOG_WARNING, "ar dhtErr no val %s %d %d", path, opState, __LINE__);
					} else {
						if (pval->m_len == sizeof(FileAttr) ){
							if (memcmp(pval->m_pVal, &_attr_does_not_exist, sizeof(FileAttr))) {
								uint64_t	attrTimeoutMillis;
                                uint64_t    modificationTimeMicros;
							
								attrTimeoutMillis = is_writable_path(path) ? ar->attrTimeoutMillis : CACHE_NO_TIMEOUT;
                                modificationTimeMicros = is_writable_path(path) ? stat_mtime_micros( &((FileAttr *)pval->m_pVal)->stat ) : CACHE_NO_MODIFICATION_TIME;

								// a normal data item, store in cache
								ar_store_attr_in_cache_static(path, (FileAttr *)pval->m_pVal, FALSE, modificationTimeMicros, attrTimeoutMillis);
								successful = TRUE;
							} else {
								// ignore not found when prefetching
							}
						} else {
							srfsLog(LOG_WARNING, "val->size() != sizeof(FileAttr) %s %d", __FILE__, __LINE__);
							pval = NULL; // FIXME - temp workaround the problem
						}
					}
					if (pval != NULL) {
						sk_destroy_val(&pval);
					}
					delete pStoredVal;
				}
            } else if (opState == SKOperationState::INCOMPLETE){
				// ignore this failure for prefetch
            } else {
				// ignore this failure for prefetch
            }
			mem_free((void **)&paths[i]);
		}
		pValRetrieval->close();
		delete pValRetrieval;
		srfsLog(LOG_FINE, "out ar_process_dht_batch");
		return;
	} catch (SKClientException & e) {
		// Shouldn't reach here as the only currently thrown exception 
		// is a RetrievalException which is handled above
		fatalError("ar unexpected SKClientException", __FILE__, __LINE__);
	}

    //srfsLog(LOG_WARNING, "ar_process_dht_batch got %d values in group %s ", pValues->size(), SKFS_ATTR_NS );
    if (!pValues){
        srfsLog(LOG_WARNING, "ar dhtErr no keys from namespace %s", SKFS_ATTR_NS);
    } else {
        OpStateMap	*opStateMap = pValRetrieval->getOperationStateMap();
        for (i = 0; i < numRequests; i++) {
			char	*path;
            int		successful;
            SKVal   *ppval;
            SKOperationState::SKOperationState opState;

            successful = FALSE;
            path = paths[i];
            
            try {
                ppval = pValues->at(path);
            } catch(std::exception& emap) { 
                ppval = NULL;
                srfsLog(LOG_INFO, "ar std::map exception at %s:%d\n%s\n", __FILE__, __LINE__, emap.what()); 
            }
            try {
                opState = opStateMap->at(path);
            } catch(std::exception& emap) { 
                opState = SKOperationState::FAILED;
                srfsLog(LOG_INFO, "ar std::map exception at %s:%d\n%s\n", __FILE__, __LINE__, emap.what()); 
            }
			srfsLog(LOG_FINE, "b: prefetch %s %d", path, opState);
            if (opState == SKOperationState::SUCCEEDED) {
                if (ppval == NULL) {
	                srfsLog(LOG_WARNING, "ar dhtErr no val %s %d line %d", path, opState,  __LINE__);
                } else if (ppval->m_len == sizeof(FileAttr) ){
		            if (memcmp(ppval->m_pVal, &_attr_does_not_exist, sizeof(FileAttr))) {
						uint64_t	attrTimeoutMillis;
                        uint64_t    modificationTimeMicros;
					
						attrTimeoutMillis = is_writable_path(path) ? ar->attrTimeoutMillis : CACHE_NO_TIMEOUT;
			            modificationTimeMicros = is_writable_path(path) ? stat_mtime_micros( &((FileAttr *)ppval->m_pVal)->stat ) : CACHE_NO_MODIFICATION_TIME;
                        // a normal data item, store in cache
			            ar_store_attr_in_cache_static(path, (FileAttr *)ppval->m_pVal, FALSE, modificationTimeMicros, attrTimeoutMillis);
			            successful = TRUE;
		            } else {
						// ignore not found when prefetching
		            }
	            } else {
		            srfsLog(LOG_WARNING, "val->size() != sizeof(FileAttr) %s %d", __FILE__, __LINE__);
					ppval = NULL; // FIXME - temp workaround the problem
	            }
            } else if (opState == SKOperationState::INCOMPLETE) {
				// ignore for prefetch
            } else {  //SKOperationState::FAILED
				// ignore for prefetch
            }
			if (ppval != NULL) {
				sk_destroy_val(&ppval);
			}
            mem_free((void **)&paths[i]);
        }
        delete opStateMap; 
        delete pValues;
    }

    pValRetrieval->close();
    delete pValRetrieval;
	srfsLog(LOG_FINE, "out ar_process_prefetch");
}

void ar_prefetch(AttrReader *ar, char *parent, char *child) {
	int		added;
	char	*_path;
	CacheReadResult	cacheResult;
	FileAttr	fa;
	
	_path = (char *)mem_alloc(strlen(parent) + 1 + strlen(child) + 1, 1);
	sprintf(_path, "%s/%s", parent, child);
	
	cacheResult = ac_read_no_op_creation(ar->attrCache, _path, &fa);
	srfsLog(LOG_FINE, "%s %d %s", _path, cacheResult, crr_strings[cacheResult]);
	if (cacheResult == CRR_NOT_FOUND) {
		added = qp_add(ar->attrPrefetchProcessor, _path);
	} else {
		added = FALSE;
	}
	if (!added) {
		mem_free((void **)&_path);
	}
}

void ar_display_stats(AttrReader *ar, int detailedStats) {
	srfsLog(LOG_WARNING, "AttrReader Stats");
	rs_display(ar->rs);
	if (detailedStats) {
		ac_display_stats(ar->attrCache);
	}
	srfsLog(LOG_WARNING, "ar ResponseTimeStats: DHT");
	rts_display(ar->rtsDHT);
	srfsLog(LOG_WARNING, "ar ResponseTimeStats: NFS");
	rts_display(ar->rtsNFS);
}
