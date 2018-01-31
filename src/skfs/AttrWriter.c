// AttrWriter.c

/////////////
// includes

#include "ActiveOp.h"
#include "ActiveOpRef.h"
#include "AttrWriter.h"
#include "FileID.h"
#include "SRFSConstants.h"
#include "Util.h"

#include "AttrWriteRequest.h"

#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <exception>
using std::exception;


///////////////////////
// private prototypes

static void aw_process_dht_batch(void **requests, int numRequests, int curThreadIndex);


////////////////////
// private globals

static int	baseBackoffMillis = 20;
static int	maxBackoff = 20000;


///////////////////
// implementation

AttrWriter *aw_new(SRFSDHT *sd) {
	AttrWriter *aw;

	aw = (AttrWriter*)mem_alloc(1, sizeof(AttrWriter));
																								// ABQ_FULL_DROP as this is only used for
																								// native-fs backed attributes
	aw->qp = qp_new_batch_processor(aw_process_dht_batch, __FILE__, __LINE__, AW_DHT_QUEUE_SIZE, ABQ_FULL_DROP, AW_DHT_THREADS, AW_MAX_BATCH_SIZE);
	aw->sd = sd;
	aw->pSession = sd_new_session(aw->sd);
	try {
		SKNamespace	*ns;
		SKNamespacePerspectiveOptions *nspOptions;
		
		ns = aw->pSession->getNamespace(SKFS_ATTR_NS);
		nspOptions = ns->getDefaultNSPOptions();
		aw->ansp = ns->openAsyncPerspective(nspOptions);
		delete ns;
	} catch(SKClientException & ex){
		srfsLog(LOG_ERROR, "aw_new exception opening namespace %s: what: %s\n", SKFS_ATTR_NS, ex.what());
		fatalError("exception in aw_new", __FILE__, __LINE__ );
	}
	return aw;
}

void aw_delete(AttrWriter **aw) {
	if (aw != NULL && *aw != NULL) {
		(*aw)->qp->running = FALSE;
		for(int i=0; i<(*aw)->qp->numThreads; i++) {
			int added = qp_add((*aw)->qp, NULL);
			if (!added) srfsLog(LOG_ERROR, "aw_delete failed to add NULL to qp\n");
		}
		qp_delete(&(*aw)->qp);
		try {
			(*aw)->ansp->close(); 
			delete (*aw)->ansp;
		} catch (std::exception & ex) {
			srfsLog(LOG_ERROR, "exception in aw_delete: what: %s\n", ex.what());
			fatalError("exception in ar_delete", __FILE__, __LINE__ );
		}
		
		if ((*aw)->pSession) {
			delete (*aw)->pSession;
			(*aw)->pSession = NULL;
		}
		
		mem_free((void **)aw);
	} else {
		fatalError("bad ptr in aw_delete");
	}
}

void aw_write_attr(AttrWriter *aw, const char *path, FileAttr *fa) {
	AttrWriteRequest	*awr;
	int					added;

	awr = awr_new(aw, path, fa);
	added = qp_add(aw->qp, awr);
	if (!added) {
		awr_delete(&awr);
	}
}

static void aw_process_dht_batch(void **requests, int numRequests, int curThreadIndex) {
	SKOperationState::SKOperationState	dhtErr;
	int					i;
	int				j;
	AttrWriter			*aw;
   	StrValMap           requestGroup;  //keys map
    int                 isDuplicate[numRequests];

	srfsLog(LOG_FINE, "in aw_process_dht_batch %d", curThreadIndex);
	aw = NULL;
    
    memset(isDuplicate, 0, sizeof(int) * numRequests);

	for (i = 0; i < numRequests; i++) {
		AttrWriteRequest	*awr;

		awr = (AttrWriteRequest *)requests[i];
		if (aw == NULL) {
			aw = awr->attrWriter;
		} else {
			if (awr->attrWriter != aw) {
				fatalError("Unexpected multiple AttrWriter in aw_process_dht_batch");
			}
		}
		srfsLog(LOG_FINE, "AttrWriter adding to group %s", awr->path );
        SKVal* pval = sk_create_val();
        //we "point to" value
        sk_set_val_zero_copy(pval, sizeof(FileAttr), (void *)(&awr->fa) );
        requestGroup.insert( StrValMap::value_type(string(awr->path), pval ));
	}

	srfsLog(LOG_FINE, "AttrWriter call mput");
	SKOperationState::SKOperationState opState = SKOperationState::INCOMPLETE;
	SKAsyncPut * pPut = NULL;
    try {
        pPut = aw->ansp->put(&requestGroup);
    	// FUTURE: could loop through completed results instead of blocking up front
        // (would pipeline the result processing)
        pPut->waitForCompletion();

		opState = pPut->getState();
        if (srfsLogLevelMet(LOG_FINE)) {
            srfsLog(LOG_FINE, "AttrWriter mput %s %d %d %d", SKFS_ATTR_NS, requestGroup.size(), numRequests, opState );
        }
		if(opState == SKOperationState::FAILED){
			OpStateMap * pOpMap = pPut->getOperationStateMap();
			for (i = 0; i < numRequests; i++) {
   				AttrWriteRequest	*awr = (AttrWriteRequest *)requests[i];
				SKOperationState::SKOperationState iop = pOpMap->at(awr->path);
				if(iop) {
					if( iop != SKOperationState::SUCCEEDED ){
						// pPut->getFailureCause(awr->path);  // FUTURE: Consider individual key errors
						srfsLog(LOG_WARNING, "AttrWriter mput state %s %d %d,  %s %d ", awr->path, 
								(requestGroup.at(awr->path))->m_len, iop,  __FILE__, __LINE__);
					}
				} else {
					srfsLog(LOG_WARNING, "AttrWriter mput state %s %d, %s %d", awr->path,
							(requestGroup.at(awr->path))->m_len,  __FILE__, __LINE__);
				}
			}
			try {
				SKFailureCause::SKFailureCause cause = pPut->getFailureCause();
    			srfsLog(LOG_WARNING, "aw failed, cause %d", cause);
			} 
			catch(SKClientException & e) { srfsLog(LOG_WARNING, "aw getFailureCause exception line %d\n%s\n", __LINE__, e.what()); }
			catch(std::exception& e) { srfsLog(LOG_WARNING, "aw getFailureCause exception line %d\n%s\n", __LINE__, e.what()); }
			delete pOpMap;
		}

	} catch (SKPutException & e) {
        srfsLog(LOG_ERROR, "aw mput dhtErr at %s:%d\n%s\n", __FILE__, __LINE__, e.what());
		srfsLog(LOG_WARNING, " %s\n",  e.getDetailedFailureMessage().c_str());
 	} catch (SKClientException & e) {
        srfsLog(LOG_ERROR, "aw mput dhtErr at %s:%d\n%s\n", __FILE__, __LINE__, e.what());
    } catch (exception & e ){
        srfsLog(LOG_ERROR, "aw mput dhtErr Exception %s", e.what());
    }

    // Check for duplicates
    if (requestGroup.size() != numRequests) {
        // Naive n^2 search for the duplicates that must exist
        for (i = 0; i < numRequests; i++) {
            for (j = i + 1; j < numRequests; j++) {
                AttrWriteRequest    *awr_i = (AttrWriteRequest *)requests[i];
                AttrWriteRequest    *awr_j = (AttrWriteRequest *)requests[j];
                
                if (!strcmp(awr_i->path, awr_j->path)) {
                    isDuplicate[j] = TRUE;
                }
            }
        }
    }
    
	if (pPut) {
		pPut->close();
		delete pPut;
	}
    for (i = numRequests - 1; i >= 0; i--) { // reverse order so that duplicates aren't deleted before we use them
		AttrWriteRequest	*awr = (AttrWriteRequest *)requests[i];
        SKVal * ppval = requestGroup.at(awr->path);
		if (ppval == NULL || ppval->m_len == 0) {
            srfsLog(LOG_WARNING, "aw unexpected NULL %s %s\n", SKFS_ATTR_NS, awr->path);
        }
        if (!isDuplicate[i]) {
            ppval->m_len = 0; 
            //m_pVal points to awr's member, which is deleted below
            ppval->m_pVal = NULL;
            sk_destroy_val( &ppval ); // FIXME - native file relay was requiring this to be c/o, checking if fixed now
        }
		// unlike AttrReader (which uses ActiveOp to perform the deletion), we must delete the requests
		awr_delete(&awr);
	}
	srfsLog(LOG_FINE, "out aw_process_dht_batch");
}

SKOperationState::SKOperationState aw_write_attr_direct(AttrWriter *aw, const char *path, FileAttr *fa, AttrCache *ac, int maxAttempts) {
	SKOperationState::SKOperationState	result;
	SKVal		*pVal;
	SKAsyncPut	*pPut;
	int			attempt;
	unsigned int	seedp;
	
	seedp = 0;
	srfsLog(LOG_FINE, "in aw_write_attr_direct");	
	pPut = NULL;
	pVal = sk_create_val();
	sk_set_val_zero_copy(pVal, sizeof(FileAttr), (void *)fa);
	srfsLog(LOG_FINE, "aw_write_attr_direct path %s pVal->m_len %u", path, pVal->m_len);	
	attempt = 0;
	do {
		try {
            if (fa != fa_get_deletion_fa()) {
                pPut = aw->ansp->put(path, pVal);
            } else {
                pPut = aw->ansp->invalidate(path);
            }
             pPut->waitForCompletion();
			result = pPut->getState();
		} catch (exception &e) {
			srfsLog(LOG_WARNING, "aw_write_attr_direct exception %s", e.what());
			result = SKOperationState::FAILED;
		}	
		if (pPut) {
			delete pPut;
		}
		if (result != SKOperationState::SUCCEEDED) {
			if (attempt > 1) {
				int	minSleepMillis;
				int	maxSleepMillis;
				
				if (seedp == 0) {
					seedp = (unsigned int)(uint64_t)(char *)path ^ (unsigned int)curTimeMillis();
				}
				if (attempt < maxAttempts - 1) {
					minSleepMillis = 0;
					maxSleepMillis = baseBackoffMillis << int_min(attempt, maxBackoff);
				} else {
					minSleepMillis = maxBackoff;
					maxSleepMillis = maxBackoff;
				}
				srfsLog(LOG_INFO, "aw_write_attr_direct put failure. Sleeping to retry: attempt %d maxSleepMillis %d", attempt, maxSleepMillis);
				sleep_random_millis(minSleepMillis, maxSleepMillis, &seedp);
			}
		}
		++attempt;
	} while (result != SKOperationState::SUCCEEDED && attempt < maxAttempts);
    pVal->m_pVal = NULL;
    sk_destroy_val(&pVal);
	if (result == SKOperationState::SUCCEEDED && ac != NULL) {
		CacheStoreResult	acResult;
		
		acResult = ac_store_raw_data(ac, (char *)path, fa_dup(fa), TRUE, 
                                curTimeMicros(), SKFS_DEF_ATTR_TIMEOUT_SECS * 1000);
		if (acResult != CACHE_STORE_SUCCESS) {
			srfsLog(LOG_ERROR, "ac_store_raw_data_failed with %d at %s %d", acResult, __FILE__, __LINE__);
			result = SKOperationState::FAILED;
		}
	}
	srfsLog(LOG_FINE, "out aw_write_attr_direct %d", result);
	return result;
}

