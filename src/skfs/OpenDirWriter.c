// OpenDirWriter.c

/////////////
// includes

#include "ActiveOp.h"
#include "ActiveOpRef.h"
#include "DirData.h"
#include "OpenDirWriter.h"
#include "FileID.h"
#include "SRFSConstants.h"
#include "Util.h"

#include "OpenDirWriteRequest.h"

#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <exception>
using std::exception;


////////////////////
// private defines

#define ODW_MIN_WRITE_INTERVAL_MILLIS 2
#define ODW_REQUEUE_DELAY_MICROS 100


///////////////////////
// private prototypes

static void odw_process_dht_batch(void **requests, int numRequests, int curThreadIndex);
static void odw_retry(OpenDirWriter *odw, OpenDirWriteRequest *odwr);
static void odw_process_retry_batch(void **requests, int numRequests, int curThreadIndex);


/////////////////
// private data

static uint64_t _minRetrySleepMillis = 100;
static uint64_t	_maxRetrySleepMillis = 10 * 1000;
static unsigned int _retrySeed;


///////////////////
// implementation

OpenDirWriter *odw_new(SRFSDHT *sd/*, DirDataReader *ddr*/, uint64_t minWriteIntervalMillis) {
	OpenDirWriter *odw;

	odw = (OpenDirWriter*)mem_alloc(1, sizeof(OpenDirWriter));
	//odw->ddr = ddr;
	odw->qp = qp_new_batch_processor(odw_process_dht_batch, __FILE__, __LINE__, ODW_DHT_QUEUE_SIZE, ABQ_FULL_BLOCK, ODW_DHT_THREADS, ODW_MAX_BATCH_SIZE);
	//odw->retryQP = qp_new_batch_processor(odw_process_retry_batch, __FILE__, __LINE__, ODW_RETRY_QUEUE_SIZE, ABQ_FULL_DROP, ODW_RETRY_THREADS, ODW_RETRY_MAX_BATCH_SIZE);
	odw->sd = sd;
    odw->minWriteIntervalMillis = minWriteIntervalMillis;
	odw->pSession = sd_new_session(odw->sd);
	try {	
		SKNamespace	*ns;
		SKNamespacePerspectiveOptions *nspOptions;
		SKPutOptions	*pPutOptions;
		
		ns = odw->pSession->getNamespace(SKFS_DIR_NS);
		nspOptions = ns->getDefaultNSPOptions();
		
		pPutOptions = nspOptions->getDefaultPutOptions();
		pPutOptions = pPutOptions->compression(defaultCompression);
		if (defaultChecksum == SKChecksumType::NONE) {
			srfsLog(LOG_WARNING, "Turning off checksums");
		}
		pPutOptions = pPutOptions->checksumType(defaultChecksum);
		nspOptions = nspOptions->defaultPutOptions(pPutOptions);
		
		odw->ansp = ns->openAsyncPerspective(nspOptions);
		delete ns;
	} catch(SKClientException &ex) {
		srfsLog(LOG_ERROR, "odw_new exception opening namespace %s: what: %s\n", SKFS_DIR_NS, ex.what());
		fatalError("exception in odw_new", __FILE__, __LINE__ );
	}
	_retrySeed = (unsigned int)curTimeMillis() ^ (unsigned int)(uint64_t)odw;
	return odw;
}

void odw_delete(OpenDirWriter **odw) {
	if (odw != NULL && *odw != NULL) {
		(*odw)->qp->running = FALSE;
		for(int i=0; i<(*odw)->qp->numThreads; i++) {
			int added = qp_add((*odw)->qp, NULL);
			if (!added) {
				srfsLog(LOG_ERROR, "odw_delete failed to add NULL to qp\n");
			}
		}
		qp_delete(&(*odw)->qp);
		try {
			(*odw)->ansp->close(); 
			delete (*odw)->ansp;
		} catch (std::exception & ex) {
			srfsLog(LOG_ERROR, "exception in odw_delete: what: %s\n", ex.what());
			fatalError("exception in ar_delete", __FILE__, __LINE__ );
		}
		
		if ((*odw)->pSession) {
			delete (*odw)->pSession;
			(*odw)->pSession = NULL;
		}
		
		mem_free((void **)odw);
	} else {
		fatalError("bad ptr in odw_delete");
	}
}

void odw_write_dir(OpenDirWriter *odw, const char *path, OpenDir *od) {
	OpenDirWriteRequest	*odwr;
	int					okToAdd;
	int					added;

	okToAdd = od_set_queued_for_write(od, TRUE);
	if (okToAdd) {
		srfsLog(LOG_FINE, "Adding od %llx od %s", od, od->path);
		odwr = odwr_new(odw, od);
		srfsLog(LOG_FINE, "new odwr %llx", odwr);
		added = qp_add(odw->qp, odwr);
		if (!added) {
			odwr_delete(&odwr);
			fatalError("Unexpected failed odw addition", __FILE__, __LINE__);
		}
	} else {
		srfsLog(LOG_INFO, "Ignoring already queued od %s", od->path);
	}
}

static void odw_process_dht_batch(void **requests, int numRequests, int curThreadIndex) {
	SKOperationState::SKOperationState	dhtErr;
	int					i;
	OpenDirWriter		*odw;
   	StrValMap           requestGroup;  //keys map
    uint64_t            preWriteTimeMillis;
    uint64_t            postWriteTimeMillis;

	srfsLog(LOG_FINE, "in odw_process_dht_batch %d", curThreadIndex);
	odw = NULL;

    preWriteTimeMillis = curTimeMillis();
    
	for (i = 0; i < numRequests; i++) {
		OpenDirWriteRequest	*odwr;
		int	okToWrite;

		odwr = (OpenDirWriteRequest *)requests[i];
		okToWrite = od_set_queued_for_write(odwr->od, FALSE);
		if (okToWrite) {
			if (odw == NULL) {
				odw = odwr->openDirWriter;
			} else {
				if (odwr->openDirWriter != odw) {
					fatalError("Unexpected multiple OpenDirWriter in odw_process_dht_batch");
				}
			}
            // We presently rate-limit updates to a single dir. This is to prevent
            // updates from consuming too much disk space. Once that issue is resolved,
            // this code may safely be removed.
            if (preWriteTimeMillis - od_getLastWriteMillis(odwr->od) < odw->minWriteIntervalMillis) {
                okToWrite = FALSE;
                odw = NULL;
                usleep(ODW_REQUEUE_DELAY_MICROS);
                odw_write_dir(odwr->openDirWriter, odwr->od->path, odwr->od);
            }
        }
		if (okToWrite) {
			DirData	*dd;
			SKVal	*pval;
			
			srfsLog(LOG_INFO, "OpenDirWriter adding to group %llx %llx %s", odwr, odwr->od, odwr->od->path );
			dd = od_get_DirData(odwr->od, TRUE);
			srfsLog(LOG_FINE, ":: %llx %llx", odwr->od->dd, dd);
			pval = sk_create_val();
			sk_set_val(pval, dd_length_with_header_and_index(dd), (void *)(dd) );
			//if (srfsLogLevelMet(LOG_INFO)) {
			//	od_display(odwr->od, stderr);
			//	dd_display(dd, stderr);
			//}
			dd_delete(&dd);
			requestGroup.insert( StrValMap::value_type(string(odwr->od->path), pval ));
		} else {
			srfsLog(LOG_INFO, "OpenDirWriter ignoring %s", odwr->od->path );
            odwr_delete(&odwr);
            requests[i] = NULL;
		}
	}
    
    if (odw != NULL) {
        srfsLog(LOG_FINE, "OpenDirWriter call mput");
        SKOperationState::SKOperationState opState = SKOperationState::INCOMPLETE;
        SKAsyncPut * pPut = NULL;
        try {
            pPut = odw->ansp->put(&requestGroup);
            // FUTURE consider changing wait to (some form of) looping through results?
            pPut->waitForCompletion();

            opState = pPut->getState();
            if (srfsLogLevelMet(LOG_FINE)) {
                srfsLog(LOG_FINE, "OpenDirWriter mput %s %d %d %d", SKFS_DIR_NS, requestGroup.size(), numRequests, opState );
            }
            if (opState == SKOperationState::FAILED){
                OpStateMap	*pOpMap = pPut->getOperationStateMap();
                for (i = 0; i < numRequests; i++) {
                    SKOperationState::SKOperationState  iop;
                    
                    OpenDirWriteRequest	*odwr = (OpenDirWriteRequest *)requests[i];
                    
                    if (odwr != NULL) {
                        try {
                            iop = pOpMap->at(odwr->od->path);
                        } catch(std::exception& emap) { 
                            iop = SKOperationState::FAILED;
                            srfsLog(LOG_INFO, "odw std::map exception at %s:%d\n%s\n", __FILE__, __LINE__, emap.what()); 
                        }
                        if (iop) {
                            if (iop != SKOperationState::SUCCEEDED) {
                                // pPut->getFailureCause(odwr->od->path);  // FUTURE: can SK get failure for individual keys
                                srfsLog(LOG_WARNING, "OpenDirWriter mput state %s %d %d,  %s %d ", odwr->od->path, 
                                        (requestGroup.at(odwr->od->path))->m_len, iop,  __FILE__, __LINE__);
                            }
                        } else {
                            srfsLog(LOG_WARNING, "OpenDirWriter mput state %s %d, %s %d", odwr->od->path,
                                    (requestGroup.at(odwr->od->path))->m_len,  __FILE__, __LINE__);
                        }
                    }
                }
                try {
                    SKFailureCause::SKFailureCause cause = pPut->getFailureCause();
                    srfsLog(LOG_WARNING, "odw failed, cause %d", cause);
                } catch(SKClientException & e) { 
                    srfsLog(LOG_WARNING, "odw getFailureCause exception line %d\n%s\n", __LINE__, e.what()); 
                } catch(std::exception& e) { 
                    srfsLog(LOG_WARNING, "odw getFailureCause exception line %d\n%s\n", __LINE__, e.what()); 
                }
                delete pOpMap;
            }
        } catch (SKPutException &e) {
            const char    *cause;
            
            cause = e.what();
            if (cause == NULL || strstr(cause, "INVALID_VERSION") == NULL) {
                srfsLog(LOG_ERROR, "odw mput dhtErr at %s:%d\n%s\n", __FILE__, __LINE__, cause);
                srfsLog(LOG_WARNING, " %s\n",  e.getDetailedFailureMessage().c_str());
            }
            /*
            Catching failed puts is deprecated for now in favor of the periodic reconciliation approach.
            {
                OpStateMap *pOpMap = pPut->getOperationStateMap();
                for (i = 0; i < numRequests; i++) {
                    OpenDirWriteRequest	*odwr = (OpenDirWriteRequest *)requests[i];
                    SKOperationState::SKOperationState iop = pOpMap->at(odwr->od->path);
                    if (iop) {
                        if (iop != SKOperationState::SUCCEEDED) {
                            requests[i] = NULL;
                            {
                                SKVal *ppval = requestGroup.at(odwr->od->path);
                                if (ppval == NULL || ppval->m_len == 0) {
                                    srfsLog(LOG_WARNING, "odw unexpected NULL %s %s\n", SKFS_DIR_NS, odwr->od->path);
                                }
                                ppval->m_len = 0; 
                                sk_destroy_val(&ppval);
                            }
                            odw_retry(odw, odwr);
                        }
                    }
                }
            }
            */
        } catch (SKClientException &e) {
            srfsLog(LOG_ERROR, "odw mput dhtErr at %s:%d\n%s\n", __FILE__, __LINE__, e.what());
        } catch (exception & e ){
            srfsLog(LOG_ERROR, "odw mput dhtErr Exception %s at", e.what(), __FILE__, __LINE__);
            fatalError("Unexpected exception", __FILE__, __LINE__);
        }

        postWriteTimeMillis = curTimeMillis();
        
        for (i = 0; i < numRequests; i++) {
            OpenDirWriteRequest	*odwr = (OpenDirWriteRequest *)requests[i];
            if (odwr != NULL) {
                try {
                    SKVal *ppval = requestGroup.at(odwr->od->path);
                    if (ppval == NULL || ppval->m_len == 0) {
                        srfsLog(LOG_WARNING, "odw unexpected NULL %s %s\n", SKFS_DIR_NS, odwr->od->path);
                    }
                    ppval->m_len = 0; 
                    sk_destroy_val(&ppval);
                    // unlike AttrReader (which uses ActiveOp to perform the deletion), we must delete the requests
                } catch (const std::out_of_range &oor) {
                    srfsLog(LOG_WARNING, "Key not found: %s. Assuming it was a duplicate", odwr->od->path);
                } catch (exception &e) {
                    srfsLog(LOG_ERROR, "odw mput dhtErr Exception %s at %s %d", e.what(), __FILE__, __LINE__);
                    fatalError("Unexpected exception", __FILE__, __LINE__);
                }
                od_setLastWriteMillis(odwr->od, postWriteTimeMillis);
                odwr_delete(&odwr);
            }
        }
        
        if (pPut) {
            pPut->close();
            delete pPut;
        }
        
    } else {
        srfsLog(LOG_FINE, "odw_process_dht_batch ignored or requeued all writes");
    }
	srfsLog(LOG_FINE, "out odw_process_dht_batch");
}

/*

Retry logic is currently deprecated in favor of periodic reconciliation

static void odw_retry(OpenDirWriter *odw, OpenDirWriteRequest *odwr) {
	int	added;
	
	srfsLog(LOG_WARNING, "Queueing retry od %s", odwr->od->path);
	if (odwr->od->queuedForWrite) { // unsafe access as hint
		added = FALSE;
	} else {
		added = qp_add(odw->retryQP, odwr);
	}
	if (!added) {
		odwr_delete(&odwr);
		srfsLog(LOG_WARNING, "Failed retry addition %s %d", __FILE__, __LINE__);
	}
}

static int odw_process_retry(OpenDirWriteRequest *odwr) {
	int		okToAdd;
	int		added;
	OpenDir	*od;

	od = odwr->od;
	okToAdd = od_set_queued_for_write(od, TRUE);
	if (okToAdd) {
		OpenDirWriter	*odw;
		
		srfsLog(LOG_WARNING, "Retrying od, updating %s", od->path);
		odw = odwr->openDirWriter;
		ddr_update_OpenDir(odw->ddr, od);
		srfsLog(LOG_WARNING, "Update complete %s", od->path);
		added = qp_add(odw->qp, odwr);
		if (!added) {
			odwr_delete(&odwr);
			fatalError("Unexpected failed process retry addition", __FILE__, __LINE__);
		}
	} else {
		srfsLog(LOG_WARNING, "Ignoring already queued od %s", od->path);
		odwr_delete(&odwr);
	}
	return okToAdd;
}

static void odw_process_retry_batch(void **requests, int numRequests, int curThreadIndex) {
	int	i;
	int	retryAttempted;

	srfsLog(LOG_WARNING, "in odw_process_retry_batch %d", curThreadIndex);
	srfsLog(LOG_WARNING, "odw_process_retry_batch sleeping");
	srfsLog(LOG_WARNING, "odw_process_retry_batch retrying %d", numRequests);
	retryAttempted = FALSE;
	for (i = 0; i < numRequests; i++) {
		OpenDirWriteRequest	*odwr;
		int	okToAdd;

		odwr = (OpenDirWriteRequest *)requests[i];
		okToAdd = odw_process_retry(odwr);
		retryAttempted = retryAttempted || okToAdd;
	}
	if (retryAttempted) {
		sleep_random_millis(_minRetrySleepMillis, _maxRetrySleepMillis, &_retrySeed);
	}
	srfsLog(LOG_WARNING, "out odw_process_retry_batch %d", curThreadIndex);
}
*/