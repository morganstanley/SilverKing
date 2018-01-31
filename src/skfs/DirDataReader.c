// DirDataReader.c

/////////////
// includes

#include "ActiveOp.h"
#include "ActiveOpRef.h"
#include "DirDataReadRequest.h"
#include "DirDataReader.h"
#include "FileID.h"
#include "G2OutputDir.h"
#include "G2TaskOutputReader.h"
#include "OpenDirWriter.h"
#include "SRFSConstants.h"
#include "Util.h"
#include "SKVersionConstraint.h"
#include "SKAsyncSingleValueRetrieval.h"

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

#define DDR_UPDATE_INTERVAL_MILLIS	(1 * 1000)
#define _DDR_UPDATE_OP_TIMEOUT_MS (5 * 60 * 1000)
#define _DDR_READ_OP_TIMEOUT_MS (20 * 60 * 1000)


///////////////////////
// private prototypes

static void ddr_update_DirData_in_cache(DirDataReadRequest *ddrr, DirData *dd, SKMetaData *metaData);
static void ddr_store_DirData_as_OpenDir_in_cache(DirDataReadRequest *ddrr, DirData *dd);
static void ddr_process_dht_batch(void **requests, int numRequests, int curThreadIndex);
static int _ddr_get_OpenDir(DirDataReader *ddr, char *path, OpenDir **od, int createIfNotFound);
static void ddr_update_dir(DirDataReader *ddr, int curThreadIndex, DirDataReadRequest *ddrr, SKMetaData	*metaData);
static SKStoredValue *ddr_retrieve_specific_dir_version(DirDataReader *ddr, int curThreadIndex, char *path, uint64_t lowerVersionLimit);
static uint64_t ddr_get_least_version(DirDataReader *ddr, int curThreadIndex, char *path);

///////////////////
// externs

extern OpenDirWriter	*od_odw;


/////////////////
// private data


///////////////////
// implementation

DirDataReader *ddr_new(SRFSDHT *sd, ResponseTimeStats *rtsDirData, OpenDirCache *openDirCache) {
	DirDataReader *ddr;

	ddr = (DirDataReader*)mem_alloc(1, sizeof(DirDataReader));
	ddr->dirDataQueueProcessor = qp_new_batch_processor(ddr_process_dht_batch, __FILE__, __LINE__, 
								DDR_DHT_QUEUE_SIZE, ABQ_FULL_DROP, DDR_DHT_THREADS, DDR_MAX_BATCH_SIZE);
	ddr->sd = sd;
	ddr->rtsDirData = rtsDirData;
	ddr->openDirCache = openDirCache;
	try {
		SKNamespacePerspectiveOptions *nspOptions;
        SKNamespace	*ns;
		int	i;
		
        nspOptions = NULL;        
		for (i = 0; i < DDR_DHT_THREADS; i++) {                
			ddr->pSession[i] = sd_new_session(ddr->sd);
            if (nspOptions == NULL) {
                SKNamespacePerspectiveOptions *_nspOptions;
                SKGetOptions	*_getOptions;
            
                ns = ddr->pSession[i]->getNamespace(SKFS_DIR_NS);
                _nspOptions = ns->getDefaultNSPOptions();
                _getOptions = _nspOptions->getDefaultGetOptions();			
                
                // We save modified get options for use in other code
                ddr->metaDataGetOptions = _getOptions->retrievalType(META_DATA);
                ddr->valueAndMetaDataGetOptions = _getOptions->retrievalType(VALUE_AND_META_DATA);
                srfsLog(LOG_WARNING, "ddr->metaDataGetOptions %s", ddr->metaDataGetOptions->toString().c_str());            
                nspOptions = _nspOptions->defaultGetOptions(ddr->metaDataGetOptions);                
                
                delete _getOptions;
                delete _nspOptions;
            }
			ddr->ansp[i] = ns->openAsyncPerspective(nspOptions);
		}
        delete ns;
        delete nspOptions;
	} catch(SKClientException & ex){
		srfsLog(LOG_ERROR, "ddr_new exception opening namespace %s: what: %s\n", SKFS_DIR_NS, ex.what());
		fatalError("exception in ddr_new", __FILE__, __LINE__ );
	}
	
	return ddr;
}

void ddr_delete(DirDataReader **ddr) {
	if (ddr != NULL && *ddr != NULL) {
		(*ddr)->dirDataQueueProcessor->running = FALSE;
		odc_delete(&(*ddr)->openDirCache);
		for (int i = 0; i < (*ddr)->dirDataQueueProcessor->numThreads; i++) {
			int added = qp_add((*ddr)->dirDataQueueProcessor, NULL);
			if (!added) {
				srfsLog(LOG_ERROR, "ddr_delete failed to add NULL to dirDataQueueProcessor\n");
			}
		}
		qp_delete(&(*ddr)->dirDataQueueProcessor);
		try {
			int	i;
			
			for (i = 0; i < DDR_DHT_THREADS; i++) {
				(*ddr)->ansp[i]->waitForActiveOps();
				(*ddr)->ansp[i]->close();
				delete (*ddr)->ansp[i];
				(*ddr)->ansp[i] = NULL;
			}
		} catch (SKRetrievalException & e ){
			srfsLog(LOG_ERROR, "ddr dht batch at %s:%d\n%s\n", __FILE__, __LINE__, e.what());
			srfsLog(LOG_ERROR, " %s\n",  e.getDetailedFailureMessage().c_str());
			fatalError("exception in ddr_delete", __FILE__, __LINE__ );
		} catch (std::exception & ex) {
			srfsLog(LOG_ERROR, "exception in ddr_delete: what: %s\n", ex.what());
			fatalError("exception in ddr_delete", __FILE__, __LINE__ );
		}
		
		if ((*ddr)->pSession) {
			int	i;
			
			for (i = 0; i < DDR_DHT_THREADS; i++) {
				if ((*ddr)->pSession[i]) {
					delete (*ddr)->pSession[i];
					(*ddr)->pSession[i] = NULL;
				}
			}
		}
		
		mem_free((void **)ddr);
	} else {
		fatalError("bad ptr in ddr_delete");
	}
}

// cache

static void ddr_store_DirData_as_OpenDir_in_cache(DirDataReadRequest *ddrr, DirData *dd) {
	CacheStoreResult	result;
	OpenDir	*od;

	srfsLog(LOG_FINE, "Storing od in cache %s", ddrr->path);
	od = od_new(ddrr->path, dd);
	result = odc_store(ddrr->dirDataReader->openDirCache, ddrr->path, od);
	if (result == CACHE_STORE_SUCCESS) {
		srfsLog(LOG_FINE, "Cache store success %s", ddrr->path);
	} else {
		srfsLog(LOG_FINE, "Cache store rejected %s", ddrr->path);
		od_delete(&od);
	}
}

/**
 * Update DirData in the cached OpenDir. (OpenDir must already be created and cached.)
 */
static void ddr_update_DirData_in_cache(DirDataReadRequest *ddrr, DirData *dd, SKMetaData *metaData) {	
	CacheReadResult	result;
	OpenDir			*_od;
	
	srfsLog(LOG_FINE, "ddr_update_DirData_in_cache %llx %s %llx", ddrr, ddrr->path, metaData);
	_od = NULL;
	result = odc_read_no_op_creation(ddrr->dirDataReader->openDirCache, ddrr->path, &_od);
	if (result != CRR_FOUND) {
        if (dd != NULL) {
            srfsLog(LOG_ERROR, "od not found for %s", ddrr->path);
            fatalError("od not found", __FILE__, __LINE__);
        } else {
            // NULL dd is simply an indication to trigger an update if local data is present
            // If no local data is present, we can ignore this
        }
	} else {
		od_add_DirData(_od, dd, metaData);
	}
	srfsLog(LOG_FINE, "out ddr_update_DirData_in_cache %llx %s %llx", ddrr, ddrr->path, metaData);
}

static void ddr_process_dht_batch(void **requests, int numRequests, int curThreadIndex) {
	SKOperationState::SKOperationState   dhtMgetErr;
	DirDataReader	*ddr;
	int				i;
	ActiveOpRef		*refs[numRequests]; // Convenience cast of requests to ActiveOpRef*
	std::set<string>	seenDirs;
	int					isFirstSeen[numRequests];
	
	srfsLog(LOG_FINE, "in ddr_process_dht_batch %d", curThreadIndex);
	ddr = NULL;
    StrVector       requestGroup;  // list of keys

	// First create a group of keys to request
	for (int i = 0; i < numRequests; i++) {
		ActiveOp		*op;
		DirDataReadRequest	*ddrr;

		refs[i] = (ActiveOpRef *)requests[i];
		op = refs[i]->ao;
		ddrr = (DirDataReadRequest *)ao_get_target(op);
		ddrr_display(ddrr, LOG_FINE);
		if (ddr == NULL) {
			ddr = ddrr->dirDataReader;
		} else {
			if (ddr != ddrr->dirDataReader) {
				fatalError("multi DirDataReader batch");
			}
		}
		srfsLog(LOG_FINE, "looking in dht for dir %s", ddrr->path);
		if (seenDirs.insert(std::string(ddrr->path)).second) {
			requestGroup.push_back(ddrr->path);
			isFirstSeen[i] = TRUE;
		} else {
			isFirstSeen[i] = FALSE;
		}
	}
    
	// Now fetch the batch from the KVS
	srfsLog(LOG_FINE, "ddr_process_dht_batch calling multi_get");
    SKAsyncRetrieval *pValRetrieval = NULL;
	StrSVMap	*pValues = NULL;
    srfsLog(LOG_INFO, "got dir nsp %s ", SKFS_DIR_NS );
    try {
	    pValRetrieval = ddr->ansp[curThreadIndex]->get(&requestGroup, ddr->metaDataGetOptions); // default options set to meta data
        pValRetrieval->waitForCompletion();
        dhtMgetErr = pValRetrieval->getState();
        srfsLog(LOG_FINE, "ddr_process_dht_batch multi_get complete %d", dhtMgetErr);
        pValues = pValRetrieval->getStoredValues();
    } catch (SKRetrievalException & e) {
        srfsLog(LOG_INFO, "ddr line %d SKRetrievalException %s\n", __LINE__, e.what());
		// The operation generated an exception. This is typically simply because
		// values were not found for one or more keys (depending on namespace options.)
		// This could also, however, be caused by a true error.
		dhtMgetErr = SKOperationState::FAILED;
	} catch (SKClientException & e) {
        srfsLog(LOG_WARNING, "ddr line %d SKClientException %s\n", __LINE__, e.what());
		dhtMgetErr = SKOperationState::FAILED;
		// Shouldn't reach here as the only currently thrown exception 
		// is a RetrievalException which is handled above
		fatalError("ddr unexpected SKClientException", __FILE__, __LINE__);
    } catch(std::exception &ex) {
		srfsLog(LOG_ERROR, "ddr exception1 processing batch %s\n", ex.what());
    } catch(...) {
		srfsLog(LOG_ERROR, "ddr exception2 processing batch\n");
	}

    if (srfsLogLevelMet(LOG_INFO)) {
        srfsLog(LOG_INFO, "ddr_process_dht_batch got %d values in group %s ", pValues->size(), SKFS_DIR_NS);
    }
    if (!pValues){
        srfsLog(LOG_WARNING, "ddr dhtErr no keys from namespace %s", SKFS_DIR_NS);
        //sd_op_failed(ddr->sd, dhtMgetErr);
        for (i = numRequests - 1; i >= 0; i--) { // in reverse order so that we know when to delete ppval
            ActiveOp		*op;

            op = refs[i]->ao;
			ao_set_complete(op);
            aor_delete(&refs[i]);
        }
    } else {
		// Walk through the map and handle each result
        OpStateMap  *opStateMap = pValRetrieval->getOperationStateMap();
        for (i = numRequests - 1; i >= 0; i--) { // in reverse order so that we know when to delete ppval
            ActiveOp    *op;
            
            op = refs[i]->ao;
            if (isFirstSeen[i]) { // process only non-duplicates
                DirDataReadRequest	*ddrr;
                int				successful;
                SKStoredValue   *ppval = NULL;
                SKOperationState::SKOperationState  opState;

                successful = FALSE;
                ddrr = (DirDataReadRequest *)ao_get_target(op);
                try {
                    opState = opStateMap->at(ddrr->path);
                } catch(std::exception& emap) { 
                    opState = SKOperationState::FAILED;
                    srfsLog(LOG_INFO, "ddr std::map exception at %s:%d\n%s\n", __FILE__, __LINE__, emap.what()); 
                }
                if (opState == SKOperationState::SUCCEEDED) {
                    try {
                        ppval = pValues->at(ddrr->path);
                    } catch(std::exception& emap) { 
                        ppval = NULL;
                        srfsLog(LOG_INFO, "ddr std::map exception at %s:%d\n%s\n", __FILE__, __LINE__, emap.what()); 
                    }
                    if (ppval == NULL ){
                        srfsLog(LOG_INFO, "ddr dhtErr no val %s %d line %d", ddrr->path, opState,  __LINE__);
                        //ddr_update_DirData_in_cache(ddrr, NULL, NULL); // Trigger an update
                    }
                } else if (opState == SKOperationState::INCOMPLETE) {
                    sd_op_failed(ddr->sd, dhtMgetErr);
                    srfsLog(LOG_FINE, "%s not found in dht. Incomplete operation state.", ddrr->path);
                } else {  //SKOperationState::FAILED
                    SKFailureCause::SKFailureCause cause = SKFailureCause::ERROR;
                    try {
                        cause = pValRetrieval->getFailureCause();
                        if (cause == SKFailureCause::MULTIPLE){
                            //sd_op_failed(ddr->sd, dhtMgetErr);
                            // non-existence can reach here
                            srfsLog(LOG_INFO, "ddr dhtErr %s %d %d %d/%d line %d", ddrr->path, opState, cause, i, numRequests, __LINE__);
                        }
                        else if (cause != SKFailureCause::NO_SUCH_VALUE) {
                            sd_op_failed(ddr->sd, dhtMgetErr);
                            srfsLog(LOG_WARNING, "ddr dhtErr %s %d %d %d/%d line %d", ddrr->path, opState, cause, i, numRequests, __LINE__);
                        } else { 
                            srfsLog(LOG_INFO, "ddr dhtErr %s %d %d %d/%d line %d", ddrr->path, opState, cause, i, numRequests, __LINE__);
                            //ddr_update_DirData_in_cache(ddrr, NULL, NULL); // Trigger an update
                        }
                    } catch(SKClientException & e) { 
                        srfsLog(LOG_ERROR, "ddr getFailureCause at %s:%d\n%s\n", __FILE__, __LINE__, e.what()); 
                        sd_op_failed(ddr->sd, dhtMgetErr);
                    } catch(std::exception& e) { 
                        srfsLog(LOG_ERROR, "ddr getFailureCause exception at %s:%d\n%s\n", __FILE__, __LINE__, e.what()); 
                        sd_op_failed(ddr->sd, dhtMgetErr);
                    }
                }
                
                // If we found a value, check for update
                if (ppval) {
                    SKMetaData	*metaData;
                    
                    metaData = ppval->getMetaData();
                    ddr_update_dir(ddr, curThreadIndex, ddrr, metaData);
                    if (metaData != NULL) {
                        delete metaData;
                        delete ppval; // skclient bug limits deletion to this case
                    }
                    //if (ppval->getValue()) { // for debugging only
                    //    fatalError("unexpected value", __FILE__, __LINE__);
                    //}
                } else {
                    // no value found; store local data in kvs
                    ddr_update_dir(ddr, curThreadIndex, ddrr, NULL);
                }
            }
			srfsLog(LOG_FINE, "set op complete %llx", op);
			ao_set_complete(op);
            aor_delete(&refs[i]);
        }
        delete opStateMap; 
        delete pValues;
    }

    delete pValRetrieval;
	srfsLog(LOG_FINE, "out ddr_process_dht_batch");
}

static void ddr_update_dir(DirDataReader *ddr, int curThreadIndex, DirDataReadRequest *ddrr, SKMetaData	*metaData) {
	CacheReadResult	result;
	OpenDir			*_od;
	
	srfsLog(LOG_FINE, "ddr_update_dir %llx %s %llx", ddrr, ddrr->path, metaData);
	_od = NULL;
	result = odc_read_no_op_creation(ddrr->dirDataReader->openDirCache, ddrr->path, &_od);
	if (result != CRR_FOUND && ddrr->type != DDRR_Initial) {
        fatalError("od not found", __FILE__, __LINE__);
	} else {
        int updateKVSWithLocal;
        
        if (ddrr->type == DDRR_Initial) {
			ddr_store_DirData_as_OpenDir_in_cache(ddrr, dd_new_empty());
            result = odc_read_no_op_creation(ddrr->dirDataReader->openDirCache, ddrr->path, &_od);
            if (result != CRR_FOUND) {
                fatalError("od not found", __FILE__, __LINE__);
            }
        }
        updateKVSWithLocal = FALSE;
        if (metaData == NULL) {
            updateKVSWithLocal = TRUE;
        } else {
            uint64_t    latestKVSVersion;
            
            latestKVSVersion = metaData->getVersion();
            if (_od->lastMergedVersion < latestKVSVersion) { 
                uint64_t    curMaxVersion;
                int         scanning;
                uint64_t    scanLimit;
            
                if (_od->lastMergedVersion == 0) {
                    scanLimit = ddr_get_least_version(ddr, curThreadIndex, ddrr->path);
                } else {
                    scanLimit = _od->lastMergedVersion;
                }
                curMaxVersion = latestKVSVersion;
                scanning = TRUE;
                while (scanning) {
                    SKStoredValue   *ppval;
                    
                    if (srfsLogLevelMet(LOG_INFO)) {
                        srfsLog(LOG_INFO, "ddrr->path %s _od->lastMergedVersion %lu curMaxVersion %lu", 
                                       ddrr->path, _od->lastMergedVersion, curMaxVersion);
                    }
                    //ppval = retrieve val with greatest version <= upperMergeLimit
                    ppval = ddr_retrieve_specific_dir_version(ddr, curThreadIndex, ddrr->path, curMaxVersion);
                    if (ppval != NULL) {
                        SKMetaData      *storedValMetaData;
                        
                        storedValMetaData = ppval->getMetaData();
                        if (storedValMetaData != NULL) {
                            if (storedValMetaData->getVersion() > _od->lastMergedVersion) { // ensure scan is active
                                SKVal	*p;
                                
                                p = ppval->getValue();
                                if (p != NULL) {
                                    DirData *dd; // deleted by the pval
                                    int _updateKVSWithLocal;
                                        
                                    _updateKVSWithLocal = FALSE;
                                    //merge storedVal
                                    dd = (DirData *)p->m_pVal;
                                    if (dd != NULL) {
                                        _updateKVSWithLocal = od_add_DirData(_od, dd, metaData);
                                        // Only update kvs if the value retrieved is the most recent value in the
                                        // kvs, and if the local DirData has updates with respect to it
                                        if (_updateKVSWithLocal && (storedValMetaData->getVersion() == latestKVSVersion)) {
                                            updateKVSWithLocal = TRUE;
                                        }
                                    } else {
                                        srfsLog(LOG_WARNING, "Unexpected NULL dd %s %d", __FILE__, __LINE__);
                                    }
                                    if (srfsLogLevelMet(LOG_INFO)) {
                                        srfsLog(LOG_INFO, "ddrr->path %s storedValMetaData->getVersion() %lu", ddrr->path, storedValMetaData->getVersion());
                                        srfsLog(LOG_INFO, "ddrr->path %s _updateKVSWithLocal %d updateKVSWithLocal %d", ddrr->path, _updateKVSWithLocal, updateKVSWithLocal);
                                    }
                                    sk_destroy_val(&p);
                                } else {
                                    srfsLog(LOG_WARNING, "Unexpected NULL p %s %d", __FILE__, __LINE__);
                                }
                                curMaxVersion = storedValMetaData->getVersion() - 1;
                                if (storedValMetaData->getVersion() == scanLimit) {
                                    if (srfsLogLevelMet(LOG_INFO)) {
                                        srfsLog(LOG_INFO, "Reached merged version scan limit %s %lu", ddrr->path, storedValMetaData->getVersion());
                                    }
                                    scanning = FALSE;
                                }
                            } else { // end of scan found
                                if (srfsLogLevelMet(LOG_INFO)) {
                                    srfsLog(LOG_INFO, "Found already merged version %s %lu", ddrr->path, storedValMetaData->getVersion());
                                }
                                scanning = FALSE;
                            }
                            delete storedValMetaData;
                        } else {
                            srfsLog(LOG_WARNING, "Unexpected NULL storedValMetaData %s %d", __FILE__, __LINE__);
                        }
                        delete ppval;
                    } else {
                        srfsLog(LOG_WARNING, "null val for %s %lu", ddrr->path, curMaxVersion);
                        scanning = FALSE;
                    }
                }
                if (!updateKVSWithLocal) {
                    _od->lastMergedVersion = latestKVSVersion;
                }
                srfsLog(LOG_INFO, "All merges complete ddrr->path %s. updateKVSWithLocal %d _od->lastMergedVersion now %lu", 
                        ddrr->path, updateKVSWithLocal, _od->lastMergedVersion);
            } else {
                srfsLog(LOG_INFO, "Already up to date ddrr->path %s", ddrr->path);
            }
        }
        
        if (updateKVSWithLocal) {
            odw_write_dir(od_odw, ddrr->path, _od);
        }
	}
	srfsLog(LOG_FINE, "out ddr_update_dir %llx %s %llx", ddrr, ddrr->path, metaData);
}

static SKStoredValue *ddr_retrieve_specific_dir_version(DirDataReader *ddr, int curThreadIndex, char *path, uint64_t upperVersionLimit) {
    SKStoredValue   *storedValue;
    SKAsyncRetrieval   *pRetrieval;
	SKOperationState::SKOperationState   dhtMgetErr;
	uint64_t		t1;
	uint64_t		t2;

    storedValue = NULL;
    pRetrieval = NULL;
    try {
        SKGetOptions	*getOptions;
        string  _path;
            
        // need to fill in operation options        
        SKVersionConstraint   *vc;
        
        vc = SKVersionConstraint::maxBelowOrEqual(upperVersionLimit);
        getOptions = ddr->valueAndMetaDataGetOptions->versionConstraint(vc);
        
	    t1 = curTimeMillis();
	    pRetrieval = ddr->ansp[curThreadIndex]->get(path, getOptions);
        pRetrieval->waitForCompletion();
	    t2 = curTimeMillis();
        rts_add_sample(ddr->rtsDirData, t2 - t1, 1);
        dhtMgetErr = pRetrieval->getState();
        srfsLog(LOG_FINE, "ddr_retrieve_specific_dir_version get complete %d", dhtMgetErr);
        _path = string(path);
        if (pRetrieval->getState() != SKOperationState::SUCCEEDED) {
            storedValue = NULL;
        } else {
            storedValue = pRetrieval->getStoredValue(_path);
        }
        
        delete getOptions;
        delete vc;
    } catch (SKRetrievalException & e) {
        srfsLog(LOG_INFO, "ddr line %d SKRetrievalException %s\n", __LINE__, e.what());
		// The operation generated an exception. This is typically simply because
		// values were not found for one or more keys (depending on namespace options.)
		// This could also, however, be caused by a true error.
		dhtMgetErr = SKOperationState::FAILED;
	} catch (SKClientException & e) {
        srfsLog(LOG_WARNING, "ddr line %d SKClientException %s\n", __LINE__, e.what());
		dhtMgetErr = SKOperationState::FAILED;
		// Shouldn't reach here as the only currently thrown exception 
		// is a RetrievalException which is handled above
		fatalError("ddr unexpected SKClientException", __FILE__, __LINE__);
	}
    delete pRetrieval;
    return storedValue;
}

static uint64_t ddr_get_least_version(DirDataReader *ddr, int curThreadIndex, char *path) {
    SKAsyncRetrieval   *pRetrieval;
	SKOperationState::SKOperationState   dhtMgetErr;
    uint64_t    leastVersion;

    leastVersion = 0;
    pRetrieval = NULL;
    try {
        SKGetOptions	*getOptions;
        string  _path;
            
        // need to fill in operation options        
        SKVersionConstraint   *vc;
        
        vc = SKVersionConstraint::minAboveOrEqual(0);
        getOptions = ddr->metaDataGetOptions->versionConstraint(vc);
        
	    pRetrieval = ddr->ansp[curThreadIndex]->get(path, getOptions);
        pRetrieval->waitForCompletion();
        dhtMgetErr = pRetrieval->getState();
        srfsLog(LOG_FINE, "ddr_retrieve_specific_dir_version get complete %d", dhtMgetErr);
        _path = string(path);
        if (pRetrieval->getState() != SKOperationState::SUCCEEDED) {
            leastVersion = 0;
        } else {
            SKStoredValue   *storedValue;
            
            storedValue = pRetrieval->getStoredValue(_path);
            if (storedValue != NULL) {
                SKMetaData  *storedValMetaData;
                
                storedValMetaData = storedValue->getMetaData();
                if (storedValMetaData != NULL) {
                    leastVersion = storedValMetaData->getVersion();
                    delete storedValMetaData;
                }
                delete storedValue;
            }
        }
        
        delete getOptions;
        delete vc;
    } catch (SKRetrievalException & e) {
        srfsLog(LOG_INFO, "ddr line %d SKRetrievalException %s\n", __LINE__, e.what());
		// The operation generated an exception. This is typically simply because
		// values were not found for one or more keys (depending on namespace options.)
		// This could also, however, be caused by a true error.
		dhtMgetErr = SKOperationState::FAILED;
	} catch (SKClientException & e) {
        srfsLog(LOG_WARNING, "ddr line %d SKClientException %s\n", __LINE__, e.what());
		dhtMgetErr = SKOperationState::FAILED;
		// Shouldn't reach here as the only currently thrown exception 
		// is a RetrievalException which is handled above
		fatalError("ddr unexpected SKClientException", __FILE__, __LINE__);
	}
    delete pRetrieval;
    return leastVersion;
}

/*
static void ddr_update_dir(DirDataReader *ddr, int curThreadIndex, DirDataReadRequest *ddrr, SKMetaData	*metaData) {
	CacheReadResult	result;
	OpenDir			*_od;
	
	srfsLog(LOG_FINE, "ddr_update_dir %llx %s %llx", ddrr, ddrr->path, metaData);
	_od = NULL;
	result = odc_read_no_op_creation(ddrr->dirDataReader->openDirCache, ddrr->path, &_od);
	if (result != CRR_FOUND && ddrr->type != DDRR_Initial) {
        fatalError("od not found", __FILE__, __LINE__);
	} else {
        int updateKVSWithLocal;
        
        if (ddrr->type == DDRR_Initial) {
			ddr_store_DirData_as_OpenDir_in_cache(ddrr, dd_new_empty());
            result = odc_read_no_op_creation(ddrr->dirDataReader->openDirCache, ddrr->path, &_od);
            if (result != CRR_FOUND) {
                fatalError("od not found", __FILE__, __LINE__);
            }
        }
        updateKVSWithLocal = FALSE;
        if (metaData == NULL) {
            updateKVSWithLocal = TRUE;
        } else {
            uint64_t    latestKVSVersion;
            
            latestKVSVersion = metaData->getVersion();
            while (_od->lastMergedVersion < latestKVSVersion) {
                uint64_t        lowerMergeLimit;
                SKStoredValue   *ppval;
                SKMetaData      *storedValMetaData;
                DirData         *dd;
				SKVal	        *p;
				
                if (srfsLogLevelMet(LOG_INFO)) {
                    srfsLog(LOG_INFO, "ddrr->path %s _od->lastMergedVersion %lu < latestKVSVersion %lu", 
                                   ddrr->path, _od->lastMergedVersion, latestKVSVersion);
                }
                lowerMergeLimit = _od->lastMergedVersion + 1;
                //ppval = retrieve val with least version >= latestKVSVersion
                ppval = ddr_retrieve_specific_dir_version(ddr, curThreadIndex, ddrr->path, lowerMergeLimit);
                if (ppval != NULL) {
                    p = ppval->getValue();
                    
                    //merge storedVal
                    dd = (DirData *)p->m_pVal;
                    updateKVSWithLocal = od_add_DirData(_od, dd, metaData);
                    storedValMetaData = ppval->getMetaData();
                    if (srfsLogLevelMet(LOG_INFO)) {
                        srfsLog(LOG_INFO, "ddrr->path %s storedValMetaData->getVersion() %lu", ddrr->path, storedValMetaData->getVersion());
                    }
                    _od->lastMergedVersion = storedValMetaData->getVersion();
                    
                    sk_destroy_val(&p);
                    delete storedValMetaData;
                    delete ppval;
                } else {
                    srfsLog(LOG_WARNING, "null val for %s %lu", ddrr->path, lowerMergeLimit);
                    fatalError("null storedValue in ddr_update_dir", __FILE__, __LINE__);
                }
            }            
            srfsLog(LOG_INFO, "All merges complete ddrr->path %s", ddrr->path);
        }
        
        if (updateKVSWithLocal) {
            odw_write_dir(od_odw, ddrr->path, _od);
        }
	}
	srfsLog(LOG_FINE, "out ddr_update_dir %llx %s %llx", ddrr, ddrr->path, metaData);
}

static SKStoredValue *ddr_retrieve_specific_dir_version(DirDataReader *ddr, int curThreadIndex, char *path, uint64_t lowerVersionLimit) {
    SKStoredValue   *storedValue;
    SKAsyncRetrieval   *pRetrieval;
	SKOperationState::SKOperationState   dhtMgetErr;
	uint64_t		t1;
	uint64_t		t2;

    storedValue = NULL;
    pRetrieval = NULL;
    try {
        SKGetOptions	*getOptions;
        string  _path;
            
        // need to fill in operation options        
        SKVersionConstraint   *vc;
        
        vc = SKVersionConstraint::minAboveOrEqual(lowerVersionLimit);
        getOptions = ddr->valueAndMetaDataGetOptions->versionConstraint(vc);
        
	    t1 = curTimeMillis();
	    pRetrieval = ddr->ansp[curThreadIndex]->get(path, getOptions);
        pRetrieval->waitForCompletion();
	    t2 = curTimeMillis();
        rts_add_sample(ddr->rtsDirData, t2 - t1, 1);
        dhtMgetErr = pRetrieval->getState();
        srfsLog(LOG_FINE, "ddr_retrieve_specific_dir_version get complete %d", dhtMgetErr);
        _path = string(path);
        storedValue = pRetrieval->getStoredValue(_path);
        
        delete getOptions;
        delete vc;
    } catch (SKRetrievalException & e) {
        srfsLog(LOG_INFO, "ddr line %d SKRetrievalException %s\n", __LINE__, e.what());
		// The operation generated an exception. This is typically simply because
		// values were not found for one or more keys (depending on namespace options.)
		// This could also, however, be caused by a true error.
		dhtMgetErr = SKOperationState::FAILED;
	} catch (SKClientException & e) {
        srfsLog(LOG_WARNING, "ddr line %d SKClientException %s\n", __LINE__, e.what());
		dhtMgetErr = SKOperationState::FAILED;
		// Shouldn't reach here as the only currently thrown exception 
		// is a RetrievalException which is handled above
		fatalError("ddr unexpected SKClientException", __FILE__, __LINE__);
	}
    delete pRetrieval;
    return storedValue;
}
*/


// Callback from Cache. Cache write lock is held during callback.
ActiveOp *ddr_create_active_op(void *_ddr, void *_path, uint64_t noMinModificationTime) {
	DirDataReader *ddr;
	char *path;
	ActiveOp *op;
	DirDataReadRequest	*dirDataReadRequest;

	ddr = (DirDataReader *)_ddr;
	path = (char *)_path;
	srfsLog(LOG_FINE, "ddr_create_active_op %s", path);
	dirDataReadRequest = ddrr_new(ddr, path, DDRR_Initial);
	ddrr_display(dirDataReadRequest, LOG_FINE);
	op = ao_new(dirDataReadRequest, (void (*)(void **))ddrr_delete);
	return op;
}

////////////////////////////////////////////////////////////

DirData *ddr_get_DirData(DirDataReader *ddr, char *path) {
	int	result;
	OpenDir	*od;

	srfsLog(LOG_FINE, "ddr_get_DirData %s", path);
	od = NULL;
	result = ddr_get_OpenDir(ddr, path, &od, DDR_NO_AUTO_CREATE);
	if (result == 0) {
		ddr_check_for_update(ddr, od);
		srfsLog(LOG_FINE, "out1 ddr_get_DirData %s", path);
		return od_get_DirData(od);
	} else {
		srfsLog(LOG_FINE, "out2 ddr_get_DirData %s", path);
		return NULL;
	}
}

void ddr_check_for_update(DirDataReader *ddr, OpenDir *od) {
	if (od_getElapsedSinceLastUpdateMillis(od) > DDR_UPDATE_INTERVAL_MILLIS) {
		ddr_update_OpenDir(ddr, od);
	}
}

void ddr_check_for_reconciliation(DirDataReader *ddr, char *path) {
	OpenDir	*od;
	int		result;

	od = NULL;
	result = ddr_get_OpenDir(ddr, path, &od, DDR_NO_AUTO_CREATE);
	if (result == 0) {
		if (od->needsReconciliation > 0) { // unsafe access, using as a hint
			srfsLog(LOG_INFO, "Reconciliation required %s", path);
			ddr_update_OpenDir(ddr, od);
		} else {
			srfsLog(LOG_INFO, "No reconciliation required %s", path);
		}
	}
}

void ddr_update_OpenDir(DirDataReader *ddr, OpenDir *od) {
	int			result;
	char		*path;
	ActiveOp 	*op;
	ActiveOpRef	*q_aor;
	int			added;
	DirDataReadRequest	*ddrr;
	ActiveOpRef	*aor;
	
	path = od->path;
	srfsLog(LOG_FINE, "ddr_update_OpenDir %s", path);		
	srfsLog(LOG_FINE, "ddr requesting DirData update for %s", path);
	ddrr = ddrr_new(ddr, path, DDRR_Update);
	op = ao_new(ddrr, (void (*)(void **))ddrr_delete);
	q_aor = aor_new(op, __FILE__, __LINE__);
	aor = aor_new(op, __FILE__, __LINE__);
	added = qp_add(ddr->dirDataQueueProcessor, q_aor);
	if (!added) {
		aor_delete(&q_aor);
		// op deletion handled automatically as always
		srfsLog(LOG_FINE, "ddr OpenDir DirData update failed for %s", path);
	} else {
		aor_wait_for_stage_timed(aor, AO_STAGE_COMPLETE, _DDR_UPDATE_OP_TIMEOUT_MS);
		srfsLog(LOG_FINE, "ddr OpenDir DirData update complete for %s", path);
	}
	aor_delete(&aor);
	srfsLog(LOG_FINE, "out1 ddr_update_OpenDir %s", path);
}

int ddr_get_OpenDir(DirDataReader *ddr, char *path, OpenDir **od, int createIfNotFound) {
	int		errorCode;

	srfsLog(LOG_FINE, "in ddr_get_OpenDir: %s", path);
	errorCode = _ddr_get_OpenDir(ddr, path, od, createIfNotFound);
	
	if (errorCode != 0) {
		// Found an error. Handle it			
		if (errorCode != ENOENT) {
			// Error is a true error
			// leave errorCode unchanged
		} else {
			// Error is ENOENT
			// leave errorCode unchanged
		}
		if (errorCode != 0 && od != NULL) {
			*od = NULL;
		}
	}
	return errorCode;
}

static int _ddr_get_OpenDir(DirDataReader *ddr, char *path, OpenDir **od, int createIfNotFound) {
	CacheReadResult	result;
	ActiveOpRef		*activeOpRef;
	int				returnCode;
	
	returnCode = 0;
	activeOpRef = NULL;
	
	// Look in the OpenDirCache for an existing operation
	// Create a new operation if none exists

	srfsLog(LOG_FINE, "looking in openDirCache for %s", path);
	result = odc_read(ddr->openDirCache, path, od, &activeOpRef, ddr);
	srfsLog(LOG_FINE, "openDirCache result %d %s", result, crr_strings[result]);
	if (result != CRR_ACTIVE_OP_CREATED) {
		// No operation was created. We must have a result, an existing operation, or an error code to return.
		if (result == CRR_FOUND) {
			// Found in cache, and hence already copied out by the read()
			// We only need to update cache stats here
			//srfsLog(LOG_FINE, "ddr_get_OpenDir inode %lu \n", stbuf->st_ino );
			srfsLog(LOG_FINE, "out ddr_get_OpenDir: %s\tfound in cache", path);
			//rs_cache_inc(ddr->rs);
			return 0;
		} else if (result == CRR_ACTIVE_OP_EXISTING) {
            // Found an existing operation.
			// Copy the data out (done below at end)
			// No fallback to native here since the initial operation will do that
			srfsLog(LOG_FINE, "waiting for existing op completion %s", path);
            aor_wait_for_stage_timed(activeOpRef, AO_STAGE_COMPLETE, _DDR_READ_OP_TIMEOUT_MS);
			aor_delete(&activeOpRef);
			//rs_opWait_inc(ddr->rs);
		} else if (result == CRR_ERROR_CODE) {
			int	errorCode;
			
			errorCode = *((int *)od);
			return errorCode;
		} else {
			// Can't legitimately reach here. All cases should have been handled above.
			fatalError("panic", __FILE__, __LINE__);
		}
	} else {
		ActiveOp *op;

		// A new operation was created. 
		
		// First look in the dht
		srfsLog(LOG_FINE, "CRR_ACTIVE_OP_CREATED. Adding to dht queue %s", path);
		op = activeOpRef->ao;

		if (!sd_is_enabled(ddr->sd)) {
			srfsLog(LOG_WARNING, "sd not enabled in DirDataReader");
		}
		
		{
			int			added;
			ActiveOpRef	*aor;

			srfsLog(LOG_FINE, "Queueing op %s %llx", path, op);
			aor = aor_new(op, __FILE__, __LINE__);
			added = qp_add(ddr->dirDataQueueProcessor, aor);
			if (!added) {
				aor_delete(&aor);
                return EAGAIN;
			}
		}
		
		srfsLog(LOG_FINE, "Waiting for dht op completion %s", path);
		aor_wait_for_stage_timed(activeOpRef, AO_STAGE_COMPLETE, _DDR_READ_OP_TIMEOUT_MS);
		srfsLog(LOG_FINE, "op dht stage complete %s", path);
		result = odc_read_no_op_creation(ddr->openDirCache, path, od);
		srfsLog(LOG_FINE, "cache result %d %s", result, crr_strings[result]);

		// Check to see if the dht had the attr
		if (result == CRR_FOUND) {
			// dht had it. we're done
			//rs_dht_inc(ddr->rs);
			srfsLog(LOG_FINE, "od found in dht");
			aor_delete(&activeOpRef);
			return 0;
		} else if (result == CRR_ACTIVE_OP_EXISTING) {
			// This means that our second check for a result failed.
			// (In this section of code, the "existing operation" was created by this call to ar_get_attr.)
			srfsLog(LOG_FINE, "%s is not native, op not found", path);
			result = CRR_NOT_FOUND;
			odc_remove_active_op(ddr->openDirCache, path);
			//srfsLog(LOG_FINE, "waiting (final) for kvs op completion %s", path);
			aor_delete(&activeOpRef);
		} else if (result == CRR_ERROR_CODE) {
			int	errorCode;

			errorCode = *((int *)od);
			return errorCode;
		} else {
			fatalError("panic", __FILE__, __LINE__);
			// shouldn't reach here since we should either find the
			// result or find the active op
		}
	}

	srfsLog(LOG_FINE, "result %d %s %s %d\n", result, path, __FILE__, __LINE__);
	// below will copy the data out if it exists
	result = odc_read_no_op_creation(ddr->openDirCache, path, od);
	srfsLog(LOG_FINE, "cache result %d %s", result, crr_strings[result]);
	switch (result) {
	case CRR_FOUND:
		returnCode = 0;
		break;
	case CRR_ERROR_CODE:
		returnCode = *((int *)od);
		break;
	case CRR_ACTIVE_OP_EXISTING:
        // We only get here if we didn't create the operation, but the data wasn't found
        // In this case, we fall through to the not found case
	case CRR_NOT_FOUND:
		returnCode = ENOENT;
		if (createIfNotFound == DDR_AUTO_CREATE) {
			CacheStoreResult	result;
			
			*od = od_new(path, NULL);
			result = odc_store(ddr->openDirCache, path, *od);
			if (result == CACHE_STORE_SUCCESS) {
				returnCode = 0;
			} else {
                CacheReadResult crr;
                
                // must have been stored by another thread
                // delete the one created here and read from the cache
                od_delete(od);
                crr = odc_read_no_op_creation(ddr->openDirCache, path, od);
                if (crr != CRR_FOUND) {
                    // Shouldn't happen since we only get here if the store was rejected
                    fatalError("odc_read_no_op_creation after odc store failed", __FILE__, __LINE__);
                } else {
                    returnCode = 0;
                }
			}
		}
		break;
	default: 
		srfsLog(LOG_WARNING, "unexpected result %d", result);
		srfsLog(LOG_WARNING, "crr_strings -> %s", crr_strings[result]);
		fatalError("panic", __FILE__, __LINE__);
	}
	if (activeOpRef != NULL) {
		srfsLog(LOG_FINE, "Deleting ref %s", path);
		aor_delete(&activeOpRef);
	}
	if (returnCode != 0 && returnCode != ENOENT) {
		srfsLog(LOG_WARNING, "path %s returnCode %d line %d\n", path, returnCode, __LINE__);
	}
	srfsLog(LOG_FINE, "out _ddr_get_OpenDir: %s\top completed. returnCode %d", path, returnCode);
	return returnCode;
}

void ddr_display_stats(DirDataReader *ddr, int detailedStats) {
	srfsLog(LOG_WARNING, "DirDataReader Stats");
	//rs_display(ddr->rs);
	if (detailedStats) {
		odc_display_stats(ddr->openDirCache);
	}
	srfsLog(LOG_WARNING, "ddr ResponseTimeStats: DHT");
	rts_display(ddr->rtsDirData);
}
