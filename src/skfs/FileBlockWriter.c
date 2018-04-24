// FileBlockWriter.c

/////////////
// includes

#include "ActiveOp.h"
#include "ActiveOpRef.h"
#include "FileBlockWriter.h"
#include "FileID.h"
#include "SRFSConstants.h"
#include "Util.h"

#include "FileBlockWriteRequest.h"

#include "skbasictypes.h"
#include "skconstants.h"
#include "SKAsyncInvalidation.h"
#include "SKClientException.h"

#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <exception>
using std::exception;


///////////////////////
// private prototypes

static void fbw_process_dht_batch(void **requests, int numRequests, int curThreadIndex);
static int fbw_write_not_sane(FileBlockWriteRequest *fbwr, size_t dataLength);
static FBW_ActiveDirectPut *fbwadp_new();
static void fbwadp_delete(FBW_ActiveDirectPut **adp);


///////////////////
// implementation

FileBlockWriter *fbw_new(SRFSDHT *sd, int useCompression, FileBlockCache *fbc, int reliableQueue) {
	FileBlockWriter *fbw;

	fbw = (FileBlockWriter*)mem_alloc(1, sizeof(FileBlockWriter));
	srfsLog(LOG_WARNING, "fbw_new reliableQueue %d", reliableQueue);
	fbw->qp = qp_new_batch_processor(fbw_process_dht_batch, __FILE__, __LINE__, FBW_DHT_QUEUE_SIZE, 
		reliableQueue ? ABQ_FULL_BLOCK : ABQ_FULL_DROP, FBW_DHT_THREADS, FBW_MAX_BATCH_SIZE);
	fbw->sd = sd;
	fbw->pSession = sd_new_session(fbw->sd);
	fbw->useCompression = useCompression;
    fbw->fbc = fbc;
    try {
		int	i;		
		SKNamespace	*ns;
		SKNamespacePerspectiveOptions *nspOptions;
		SKPutOptions	*pPutOptions;
		
		ns = fbw->pSession->getNamespace(SKFS_FB_NS);
		nspOptions = ns->getDefaultNSPOptions();
		
		pPutOptions = nspOptions->getDefaultPutOptions();
		pPutOptions = pPutOptions->compression(defaultCompression);
		if (defaultChecksum == SKChecksumType::NONE) {
			srfsLog(LOG_WARNING, "Turning off checksums");
		}
		pPutOptions = pPutOptions->checksumType(defaultChecksum);
		nspOptions = nspOptions->defaultPutOptions(pPutOptions);
		
		fbw->ansp = ns->openAsyncPerspective(nspOptions);
		delete ns;
		ns = NULL;
		for (i = 0; i < FBW_DHT_SESSIONS; i++) {
			fbw->_pSession[i] = sd_new_session(fbw->sd);
			
			ns = fbw->_pSession[i]->getNamespace(SKFS_FB_NS);
			fbw->_ansp[i] = ns->openAsyncPerspective(nspOptions);
			delete ns;
		}
    } catch(std::exception &ex) {
		srfsLog(LOG_ERROR, "fbw_new exception opening namespace %s: what: %s\n", SKFS_FB_NS, ex.what());
        fatalError("exception in fbw_new openAsyncNamespacePerspective", __FILE__, __LINE__ );
    } catch(...) {
        fatalError("unknown exception in fbw_new openAsyncNamespacePerspective", __FILE__, __LINE__ );
    }
	return fbw;
}

void fbw_delete(FileBlockWriter **fbw) {
	if (fbw != NULL && *fbw != NULL) {
		(*fbw)->qp->running = FALSE;
		for(int i=0; i<FBW_DHT_THREADS; i++) {
			int added = qp_add((*fbw)->qp, NULL);
			if (!added) srfsLog(LOG_ERROR, "fbw_delete failed to add NULL to qp\n");
		}
		try {
			int	i;
			
			//(*fbw)->ansp->close();
		    delete (*fbw)->ansp;
			for (i = 0; i < FBW_DHT_SESSIONS; i++) {
				(*fbw)->_ansp[i]->waitForActiveOps();
				(*fbw)->_ansp[i]->close();
				delete (*fbw)->_ansp[i];
			}
		} catch (std::exception & ex) {
			srfsLog(LOG_WARNING, "exception in fbw_delete: what: %s\n", ex.what());
			fatalError("exception in fbw_delete", __FILE__, __LINE__ );
		}
		qp_delete(&(*fbw)->qp);
		
		if ((*fbw)->pSession) {
			delete (*fbw)->pSession;
			(*fbw)->pSession = NULL;
		}
		
		if ((*fbw)->_pSession) {
			int	i;
			
			for (i = 0; i < FBW_DHT_SESSIONS; i++) {
				if ((*fbw)->_pSession[i]) {
					delete (*fbw)->_pSession[i];
					(*fbw)->_pSession[i] = NULL;
				}
			}
		}		
		
		mem_free((void **)fbw);
	} else {
		fatalError("bad ptr in fbw_delete");
	}
}

static int fbw_write_not_sane(FileBlockWriteRequest *fbwr) {
	if (fid_is_native_fs(&fbwr->fbid->fid)) {
		return (fbwr->dataLength > SRFS_BLOCK_SIZE) || (!fbid_is_last_block(fbwr->fbid) && fbwr->dataLength != SRFS_BLOCK_SIZE);
	} else {
		// SKFS file IDs contain no length information, so we can't sanity check using it.
		return fbwr->dataLength > SRFS_BLOCK_SIZE;
	}
}

void fbw_write_file_block(FileBlockWriter *fbw, FileBlockID *fbid, size_t dataLength, void *data, ActiveOpRef *aor) {
	FileBlockWriteRequest	*fbwr;
	int	added;

	fbwr = fbwr_new(fbw, fbid, dataLength, data, aor);
	if (fbw_write_not_sane(fbwr)) {
		char	key[SRFS_FBID_KEY_SIZE];

		fbid_to_string(fbwr->fbid, key);
		srfsLog(LOG_ERROR, "Bogus block write attempt %lu %s", fbwr->dataLength, key);
		fatalError("Bogus block write attempt", __FILE__, __LINE__);
	} else {
		if (srfsLogLevelMet(LOG_FINE)) {
			char	key[SRFS_FBID_KEY_SIZE];

			fbid_to_string(fbwr->fbid, key);
			srfsLog(LOG_FINE, "Clean write attempt %lu %s", fbwr->dataLength, key);
		}
	}
	added = qp_add(fbw->qp, fbwr);
	if (!added) {
		fbwr_delete(&fbwr);
    }
}

static void fbw_process_dht_batch(void **requests, int numRequests, int curThreadIndex) {
	SKOperationState::SKOperationState	dhtErr = SKOperationState::INCOMPLETE;
   	StrValMap           requestGroup;  // map of keys 
	int					i;
	int				    j;
	FileBlockWriter		*fbw;
	char				keys[numRequests][SRFS_FBID_KEY_SIZE];
    int                 isDuplicate[numRequests];

	srfsLog(LOG_FINE, "in fbw_process_dht_batch %d", curThreadIndex);
	fbw = NULL;
    char * ns = NULL;
    
    memset(isDuplicate, 0, sizeof(int) * numRequests);

    // First, construct the requestGroup
	for (i = 0; i < numRequests; i++) {
		FileBlockWriteRequest	*fbwr;

		fbwr = (FileBlockWriteRequest *)requests[i];
		if (fbw == NULL) {
			fbw = fbwr->fileBlockWriter;
		} else {
			if (fbwr->fileBlockWriter != fbw) {
				fatalError("Unexpected multiple FileBlockWriter in fbw_process_dht_batch");
			}
		}
		fbid_to_string(fbwr->fbid, keys[i]);
		if (fbw_write_not_sane(fbwr)) {
			srfsLog(LOG_ERROR, "Bogus block write attempt %lu %s", fbwr->dataLength, keys[i]);
			fatalError("Bogus block write attempt", __FILE__, __LINE__);
		}
		srfsLog(LOG_FINE, "fbw adding to group %llx %s %llx %d", keys[i], keys[i], fbwr->data, fbwr->dataLength);
        SKVal* pval = sk_create_val();
        sk_set_val_zero_copy(pval, fbwr->dataLength, (void *)(fbwr->data) );
        requestGroup.insert( StrValMap::value_type( string(keys[i]), pval));
	}
    srfsLog(LOG_FINE, "fbw mput %d %d ", numRequests, requestGroup.size());

    // Second, store the group into the key-value store
	SKAsyncPut * pPut = NULL;
    try {
		OpStateMap  *pOpMap;
        
        pPut = fbw->ansp->put(&requestGroup);
        // FUTURE - could process partial completion instead of blocking for all
        pPut->waitForCompletion();

		dhtErr = pPut->getState();
		srfsLog(LOG_FINE, "fbw mput complete: %d", dhtErr);
		pOpMap = pPut->getOperationStateMap();
		for (i = 0; i < numRequests; i++) {
			OpStateMap::iterator    osmit;
            
			//FileBlockWriteRequest	*fbwr = (FileBlockWriteRequest *)requests[i];
			osmit = pOpMap->find(keys[i]);
			if (osmit != pOpMap->end()) {
				SKOperationState::SKOperationState  piop;
                
				piop = osmit->second;
				if (piop == SKOperationState::SUCCEEDED) {
					// pPut->getFailureCause(keys[i]);  //TODO: failure for individ keys
					srfsLog(LOG_FINE, " fbw %s %s %d %d", SKFS_FB_NS, keys[i], 
							(requestGroup.at(keys[i]))->m_len, piop);
				}
			} else {
				srfsLog(LOG_WARNING, "failed to write file block into dht %s %s %d", SKFS_FB_NS, keys[i],
					(requestGroup.at(keys[i]))->m_len);
			}
		}
		try {
			if (dhtErr == SKOperationState::FAILED) {
				SKFailureCause::SKFailureCause cause = pPut->getFailureCause();
				srfsLog(LOG_WARNING, "fbw failed, failure cause %d", cause);
			}
		} catch(SKClientException & e) {
            srfsLog(LOG_ERROR, "fbw getFailureCause at %s:%d\n%s\n", __FILE__, __LINE__, e.what()); 
        } catch(std::exception& e) {
            srfsLog(LOG_ERROR, "fbw getFailureCause exception at %s:%d\n%s\n", __FILE__, __LINE__, e.what());
        } catch (...) {
            srfsLog(LOG_WARNING, "fbw failed to query FailureCause"); 
        }
		delete pOpMap;
	} catch (SKPutException &e) {
        try {
            if (srfsLogLevelMet(LOG_INFO)) {
                srfsLog(LOG_INFO, "fbw mput dhtErr SKPutException at %s:%d\n%s\n", __FILE__, __LINE__, e.what());
                srfsLog(LOG_INFO, " %s\n",  e.getDetailedFailureMessage().c_str());
                e.printStackTrace();
            }
            for (i = 0; i < numRequests; i++) {
                SKOperationState::SKOperationState    opState;
                
                try {
                    opState = e.getOperationState(keys[i]);
                    if (opState != SKOperationState::SUCCEEDED) {
                        try {
                            SKFailureCause::SKFailureCause      failureCause;
                            LogLevel            logLevel;
                            
                            failureCause = e.getFailureCause(keys[i]);
                            if (failureCause == SKFailureCause::INVALID_VERSION) {
                                logLevel = LOG_FINE;
                            } else {
                                logLevel = LOG_ERROR;
                            }
                            srfsLog(logLevel, "fbw write failed for block %s cause %d", keys[i], failureCause);
                        } catch (...) {
                            srfsLog(LOG_WARNING, "fbw failed to query FailureCause %s %s %d", keys[i], __FILE__, __LINE__); 
                        }
                    }
                } catch (...) {
                    srfsLog(LOG_WARNING, "fbw failed to query OperationState %s %s %d", keys[i], __FILE__, __LINE__); 
                }
            }
        } catch (...) {
            srfsLog(LOG_WARNING, "fbw failed to in error processing %s %d", __FILE__, __LINE__);         
        }
	} catch (SKClientException & e) {
        srfsLog(LOG_WARNING, "fbw mput dhtErr SKClientException at %s:%d\n%s\n", __FILE__, __LINE__, e.what());
        e.printStackTrace();
    } catch (exception & e) {
        srfsLog(LOG_WARNING, "fbw mput dhtErr: %s", e.what());
		dhtErr = SKOperationState::FAILED;
    }
    srfsLog(LOG_FINE, "fbw mput out of process block %d %d ", numRequests, requestGroup.size());
	
	if (pPut){
		//pPut->close();
		delete pPut;
	}
    
    // Check for duplicates
    if (requestGroup.size() != numRequests) {
        // Naive n^2 search for the duplicates that must exist
        for (i = 0; i < numRequests; i++) {
            for (j = i + 1; j < numRequests; j++) {
                if (!strcmp(keys[i], keys[j])) {
                    isDuplicate[j] = TRUE;
                }
            }
        }
    }
    
    for (i = numRequests - 1; i >= 0; i--) { // reverse order so that duplicates aren't deleted before we use them
		SKVal   *ppval;
        
		ppval = requestGroup.at(keys[i]);
		if (ppval == NULL) {
			srfsLog(LOG_WARNING, "fbw unexpected ppval == NULL %s %s\n", SKFS_FB_NS, keys[i]);
		} else {
            if (ppval->m_len == 0) {
                srfsLog(LOG_WARNING, "fbw unexpected ppval->m_len == 0 %s %s\n", SKFS_FB_NS, keys[i]);
            }
        }
        if (!isDuplicate[i]) {
            ppval->m_len = 0; 
            //m_pVal points to fbwr's member, which is deleted below
            ppval->m_pVal = NULL;
            sk_destroy_val( &ppval ); // FIXME - double free check, was commented out before fix
        }
	}
	// like AttrWriter, and unlike the FileBlockReader/AttrReader, we must delete requests here
	for (i = 0; i < numRequests; i++) {
		FileBlockWriteRequest	*fbwr;

		fbwr = (FileBlockWriteRequest *)requests[i];
		fbwr_delete(&fbwr); 
	}
	srfsLog(LOG_FINE, "out fbw_process_dht_batch");
}

static uint64_t	_fbw_round_robin;

FBW_ActiveDirectPut *fbw_put_direct(FileBlockWriter *fbw, FileBlockID *fbid, WritableFileBlock *wfb) {
	FBW_ActiveDirectPut	*adp;
	
	srfsLog(LOG_FINE, "in fbw_put_direct");	
	adp = fbwadp_new();
	fbid_to_string(fbid, adp->key);
	adp->pVal = sk_create_val();
	sk_set_val_zero_copy(adp->pVal, wfb->size, (void *)wfb->block);	
	if (srfsLogLevelMet(LOG_FINE)) {
		srfsLog(LOG_FINE, "fbw_put_direct adp->key %s wfb->size %u pVal->m_len %u", adp->key, wfb->size, adp->pVal->m_len);	
	}
    // Below is a bit redundant since the micro resolution solves this for now
    // Using this for now, but could comment out as long as 1us resolution is sufficient
    fbc_remove(fbw->fbc, fbid, FALSE); // Don't delete any ongoing ops
    try {
        //adp->pPut = fbw->ansp->put(adp->key, adp->pVal);
        //adp->pPut = fbw->_ansp[fbid_hash(fbid) % FBW_DHT_SESSIONS]->put(adp->key, adp->pVal);
        adp->pPut = fbw->_ansp[_fbw_round_robin++ % FBW_DHT_SESSIONS]->put(adp->key, adp->pVal);
    } catch (exception &e) {
        srfsLog(LOG_WARNING, "fbw_put_direct put exception %s", e.what());
    }	
	srfsLog(LOG_FINE, "out fbw_put_direct");
	return adp;
}

SKOperationState::SKOperationState fbw_wait_for_direct_put(FileBlockWriter *fbw, FBW_ActiveDirectPut **_adp) {
	SKOperationState::SKOperationState	dhtErr = SKOperationState::INCOMPLETE;
	FBW_ActiveDirectPut *adp;
	
	srfsLog(LOG_FINE, "in fbw_wait_for_direct_put");
	adp = *_adp;
    try {
        adp->pPut->waitForCompletion();
		dhtErr = adp->pPut->getState();
		if (dhtErr == SKOperationState::SUCCEEDED) {
			if (srfsLogLevelMet(LOG_FINE)) {
				srfsLog(LOG_FINE, "fbw %s SUCCEEDED", adp->key);
			}
		} else {
			srfsLog(LOG_WARNING, "fbw %s failed %d %s", adp->key, dhtErr, adp->pPut->getFailureCause());
		}
	} catch (SKPutException &e) {
        srfsLog(LOG_WARNING, "fbw fbw_wait_for_direct_put SKPutException at %s:%d\n%s\n", __FILE__, __LINE__, e.what());
		srfsLog(LOG_WARNING, " %s\n",  e.getDetailedFailureMessage().c_str());
        e.printStackTrace();
	} catch (SKClientException &e) {
        srfsLog(LOG_WARNING, "fbw fbw_wait_for_direct_put SKClientException at %s:%d\n%s\n", __FILE__, __LINE__, e.what());
        e.printStackTrace();
    } catch (exception &e) {
        srfsLog(LOG_WARNING, "fbw fbw_wait_for_direct_put: %s", e.what());
		dhtErr = SKOperationState::FAILED;
    }
	fbwadp_delete(_adp);
	srfsLog(LOG_FINE, "out fbw_wait_for_direct_put");
	return dhtErr;
}

static FBW_ActiveDirectPut *fbwadp_new() {
	FBW_ActiveDirectPut *adp;
	
	adp = (FBW_ActiveDirectPut *)mem_alloc(1, sizeof(FBW_ActiveDirectPut));
	return adp;
}

static void fbwadp_delete(FBW_ActiveDirectPut **adp) {
	if (adp != NULL && *adp != NULL) {
		if (srfsLogLevelMet(LOG_FINE)) {
			srfsLog(LOG_FINE, "adp %llx *adp %llx", adp, *adp);
		}
		if ((*adp)->pPut){
			if (srfsLogLevelMet(LOG_FINE)) {
				srfsLog(LOG_FINE, "(*adp)->pPut %llx", (*adp)->pPut);
			}
			delete (*adp)->pPut;
		}
		if ((*adp)->pVal) {
			if (srfsLogLevelMet(LOG_FINE)) {
				srfsLog(LOG_FINE, "(*adp)->pVal %llx", (*adp)->pVal);
			}
			//sk_destroy_val(&(*adp)->pVal);
		}
		mem_free((void **)adp);
	} else {
		fatalError("bad ptr in adp_delete");
	}
}

////////////////////////
// File block deletion

void fbw_invalidate_file_blocks(FileBlockWriter *fbw, FileID *fid, int numRequests) {
	SKOperationState::SKOperationState	dhtErr = SKOperationState::INCOMPLETE;
   	StrVector           requestGroup;  // sets of keys 
	int					i;
	char				**keys;

	srfsLog(LOG_FINE, "in fbw_invalidate_file_blocks");

    keys = str_alloc_array(numRequests, SRFS_FBID_KEY_SIZE);
    
    // First, construct the requestGroup
	for (i = 0; i < numRequests; i++) {
        FileBlockID *fbid;
        
        fbid = fbid_new(fid, i);
		fbid_to_string(fbid, keys[i]);
		srfsLog(LOG_FINE, "fbw inv adding to group %llx %s", keys[i], keys[i]);
        requestGroup.push_back(keys[i]);
        fbid_delete(&fbid);
	}

    // Second, invalidate the group in the key-value store
	SKAsyncInvalidation *pInvalidation = NULL;
    try {
		OpStateMap  *pOpMap;
        
		srfsLog(LOG_FINE, "fbw inv calling invalidate()");
        pInvalidation = fbw->ansp->invalidate(&requestGroup);
		srfsLog(LOG_FINE, "fbw inv pInvalidation->waitForCompletion");
        pInvalidation->waitForCompletion();

		dhtErr = pInvalidation->getState();
		srfsLog(LOG_FINE, "fbw invalidation complete: %d", dhtErr);
		pOpMap = pInvalidation->getOperationStateMap();
		for (i = 0; i < numRequests; i++) {
			OpStateMap::iterator    osmit;
            
			osmit = pOpMap->find(keys[i]);
			if (osmit != pOpMap->end()) {
				SKOperationState::SKOperationState  piop;
                
				piop = osmit->second;
				if (piop == SKOperationState::SUCCEEDED) {
					// pInvalidation->getFailureCause(keys[i]);  //TODO: failure for individ keys
					srfsLog(LOG_FINE, " fbw inv %s %s %d", SKFS_FB_NS, keys[i], 
							piop);
				}
			} else {
				srfsLog(LOG_WARNING, "failed to invalidate file block in dht %s %s", SKFS_FB_NS, keys[i]);
			}
		}
		try {
			if (dhtErr == SKOperationState::FAILED) {
				SKFailureCause::SKFailureCause cause = pInvalidation->getFailureCause();
				srfsLog(LOG_WARNING, "fbw inv failed, failure cause %d", cause);
			}
		} catch(SKClientException & e) {
            srfsLog(LOG_ERROR, "fbw inv getFailureCause at %s:%d\n%s\n", __FILE__, __LINE__, e.what()); 
        } catch(std::exception& e) {
            srfsLog(LOG_ERROR, "fbw inv getFailureCause exception at %s:%d\n%s\n", __FILE__, __LINE__, e.what());
        } catch (...) {
            srfsLog(LOG_WARNING, "fbw inv failed to query FailureCause"); 
        }
		delete pOpMap;
	} catch (SKPutException &e) {
        try {
            if (srfsLogLevelMet(LOG_INFO)) {
                srfsLog(LOG_INFO, "fbw invalidation dhtErr SKPutException at %s:%d\n%s\n", __FILE__, __LINE__, e.what());
                srfsLog(LOG_INFO, " %s\n",  e.getDetailedFailureMessage().c_str());
                e.printStackTrace();
            }
            for (i = 0; i < numRequests; i++) {
                SKOperationState::SKOperationState    opState;
                
                try {
                    opState = e.getOperationState(keys[i]);
                    if (opState != SKOperationState::SUCCEEDED) {
                        try {
                            SKFailureCause::SKFailureCause      failureCause;
                            LogLevel            logLevel;
                            
                            failureCause = e.getFailureCause(keys[i]);
                            if (failureCause == SKFailureCause::INVALID_VERSION) {
                                logLevel = LOG_FINE;
                            } else {
                                logLevel = LOG_ERROR;
                            }
                            srfsLog(logLevel, "fbw invalidation failed for block %s cause %d", keys[i], failureCause);
                        } catch (...) {
                            srfsLog(LOG_WARNING, "fbw inv failed to query FailureCause %s %s %d", keys[i], __FILE__, __LINE__); 
                        }
                    }
                } catch (...) {
                    srfsLog(LOG_WARNING, "fbw inv failed to query OperationState %s %s %d", keys[i], __FILE__, __LINE__); 
                }
            }
        } catch (...) {
            srfsLog(LOG_WARNING, "fbw inv failed to in error processing %s %d", __FILE__, __LINE__);         
        }
	} catch (SKClientException & e) {
        srfsLog(LOG_WARNING, "fbw invalidation dhtErr SKClientException at %s:%d\n%s\n", __FILE__, __LINE__, e.what());
        e.printStackTrace();
    } catch (exception & e) {
        srfsLog(LOG_WARNING, "fbw invalidation dhtErr: %s", e.what());
		dhtErr = SKOperationState::FAILED;
    }
    srfsLog(LOG_FINE, "fbw invalidation out of process block %d %d ", numRequests, requestGroup.size());
	
	if (pInvalidation){
		//pInvalidation->close();
		delete pInvalidation;
	}
    str_free_array(&keys, numRequests);    
	srfsLog(LOG_FINE, "out fbw_invalidate_file_blocks");
}

