// DirDataWriter.c

/////////////
// includes

#include "ActiveOp.h"
#include "ActiveOpRef.h"
#include "DirData.h"
#include "DirDataWriter.h"
#include "SRFSConstants.h"
#include "Util.h"

#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <exception>
using std::exception;


//////////////////
// private types

typedef struct DirDataWriteRequest {
    DirDataWriter   *ddw;
    char    dirName[SRFS_MAX_PATH_LENGTH];
    DirData *dirData;
} OpenDirWriteRequest;



///////////////////////
// private prototypes

static void ddw_process_dht_batch(void **requests, int numRequests, int curThreadIndex);
static void ddw_retry(DirDataWriter *ddw, OpenDirWriteRequest *ddwr);
static void ddw_process_retry_batch(void **requests, int numRequests, int curThreadIndex);
static QueueProcessor *ddw_select_qp(DirDataWriter *ddw, OpenDirWriteRequest *ddwr);
static void ddw_queue_update(DirDataWriter *ddw, DirDataWriteRequest *ddwr);

/////////////////
// private data

static uint64_t _minRetrySleepMillis = 100;
static uint64_t    _maxRetrySleepMillis = 10 * 1000;
static unsigned int _retrySeed;


///////////////////
// implementation

// ddwr

static DirDataWriteRequest *ddwr_new(DirDataWriter *ddw, char *dirName, DirData *dirData) {
    DirDataWriteRequest *ddwr;

    ddwr = (DirDataWriteRequest*)mem_alloc(1, sizeof(DirDataWriteRequest));
    strncpy(ddwr->dirName, dirName, SRFS_MAX_PATH_LENGTH);
    ddwr->dirData = dirData;
    ddwr->ddw = ddw;
    return ddwr;
}

static DirDataWriteRequest *ddwr_create(DirDataWriter *ddw, char *dirName, uint32_t type, uint64_t version, char *entryName) {
    DirData *dd0;
    DirData *dd1;
    OpenDirUpdate   odu;
    
    dd0 = dd_new_empty();
    odu_init(&odu, type, version, entryName);
    dd1 = dd_process_updates(dd0, &odu, 1);
    dd_delete(&dd0);
    return ddwr_new(ddw, dirName, dd1);
}

static void ddwr_delete(DirDataWriteRequest **ddwr) {
    if (ddwr != NULL && *ddwr != NULL) {
        if ((*ddwr)->dirData != NULL) {
            dd_delete(&(*ddwr)->dirData);
        }
        mem_free((void **)ddwr);
    } else {
        fatalError("bad ptr in ddwr_delete");
    }
}

// ddw

DirDataWriter *ddw_new(SRFSDHT *sd) {
    DirDataWriter *ddw;
    int i;

    ddw = (DirDataWriter*)mem_alloc(1, sizeof(DirDataWriter));
    for (i = 0; i < DDW_DHT_QUEUE_PROCESSORS; i++) {
        ddw->qp[i] = qp_new_batch_processor(ddw_process_dht_batch, __FILE__, __LINE__, DDW_DHT_QUEUE_SIZE, ABQ_FULL_BLOCK, DDW_DHT_THREADS_PER_QUEUE_PROCESSOR, DDW_MAX_BATCH_SIZE);
    }
    //ddw->retryQP = qp_new_batch_processor(ddw_process_retry_batch, __FILE__, __LINE__, DDW_RETRY_QUEUE_SIZE, ABQ_FULL_DROP, DDW_RETRY_THREADS, DDW_RETRY_MAX_BATCH_SIZE);
    ddw->sd = sd;
    ddw->pSession = sd_new_session(ddw->sd);
    try {    
        SKNamespace    *ns;
        SKNamespacePerspectiveOptions *nspOptions;
        SKPutOptions    *pPutOptions;
        
        ns = ddw->pSession->getNamespace(SKFS_DIR_NS);
        nspOptions = ns->getDefaultNSPOptions();
        
        pPutOptions = nspOptions->getDefaultPutOptions();
        pPutOptions = pPutOptions->compression(defaultCompression);
        if (defaultChecksum == SKChecksumType::NONE) {
            srfsLog(LOG_WARNING, "Turning off checksums");
        }
        pPutOptions = pPutOptions->checksumType(defaultChecksum);
        nspOptions = nspOptions->defaultPutOptions(pPutOptions);
        
        ddw->ansp = ns->openAsyncPerspective(nspOptions);
        delete ns;
    } catch(SKClientException &ex) {
        srfsLog(LOG_ERROR, "ddw_new exception opening namespace %s: what: %s\n", SKFS_DIR_NS, ex.what());
        fatalError("exception in ddw_new", __FILE__, __LINE__ );
    }
    _retrySeed = (unsigned int)curTimeMillis() ^ (unsigned int)(uint64_t)ddw;
    return ddw;
}

void ddw_delete(DirDataWriter **ddw) {
    if (ddw != NULL && *ddw != NULL) {
        // FUTURE - deletion is presently unused; implement for qp if needed
        /*
        (*ddw)->qp->running = FALSE;
        for(int i=0; i<(*ddw)->qp->numThreads; i++) {
            int added = qp_add((*ddw)->qp, NULL);
            if (!added) {
                srfsLog(LOG_ERROR, "ddw_delete failed to add NULL to qp\n");
            }
        }
        qp_delete(&(*ddw)->qp);
        try {
            (*ddw)->ansp->close(); 
            delete (*ddw)->ansp;
        } catch (std::exception & ex) {
            srfsLog(LOG_ERROR, "exception in ddw_delete: what: %s\n", ex.what());
            fatalError("exception in ar_delete", __FILE__, __LINE__ );
        }
        
        if ((*ddw)->pSession) {
            delete (*ddw)->pSession;
            (*ddw)->pSession = NULL;
        }
        
        mem_free((void **)ddw);
        */
    } else {
        fatalError("bad ptr in ddw_delete");
    }
}

void ddw_update_dir(DirDataWriter *ddw, char *dirName, uint32_t type, uint64_t version, char *entryName) {
    ddw_queue_update(ddw, ddwr_create(ddw, dirName, type, version, entryName));
}

static void ddw_queue_update(DirDataWriter *ddw, DirDataWriteRequest *ddwr) {
    QueueProcessor  *qp;
    int added;
    
    qp = ddw_select_qp(ddw, ddwr);
    srfsLog(LOG_FINE, "ddw_queue_update %d %llx", ddwr->dirData->numEntries, qp);
    //dd_display(ddwr->dirData);
    added = qp_add(qp, ddwr);
    if (!added) {
        ddwr_delete(&ddwr);
        fatalError("Unexpected failed ddw addition", __FILE__, __LINE__);
    }
}

static QueueProcessor *ddw_select_qp(DirDataWriter *ddw, DirDataWriteRequest *ddwr) {
    unsigned int    i;
    
    i = stringHash(ddwr->dirName) % DDW_DHT_QUEUE_PROCESSORS;
    return ddw->qp[i];
}

static void ddw_process_dht_batch(void **requests, int numRequests, int curThreadIndex) {
    SKOperationState::SKOperationState    dhtErr;
    int                    i;
    DirDataWriter        *ddw;
       StrValMap           requestGroup;  //keys map
    uint64_t            preWriteTimeMillis;
    uint64_t            postWriteTimeMillis;
   
    srfsLog(LOG_FINE, "ddw_process_dht_batch %d %d", numRequests, curThreadIndex);
    ddw = NULL;

    preWriteTimeMillis = curTimeMillis();
    
    for (i = 0; i < numRequests; i++) {
        DirDataWriteRequest    *ddwr;
        boost::unordered_map<std::string, SKVal*>::iterator itr; 

        ddwr = (DirDataWriteRequest *)requests[i];        
        srfsLog(LOG_FINE, "processing %s %d", ddwr->dirName, ddwr->dirData->numEntries);
        //dd_display(ddwr->dirData);    
        srfsLog(LOG_INFO, "DirDataWriter adding to group %llx %llx %s", ddwr, ddwr->dirData, ddwr->dirName);
        
        itr = requestGroup.find(string(ddwr->dirName));
        if (itr != requestGroup.end() && itr->second != NULL) {
            DirData *existingDD;
            MergeResult mr;
            SKVal   *existingVal;
            
            existingVal = itr->second;
            existingDD = (DirData*)existingVal->m_pVal;
            //srfsLog(LOG_WARNING, "merge %llx %llx", ddwr->dirData, existingDD);
            mr = dd_merge(ddwr->dirData, existingDD);
            //srfsLog(LOG_WARNING, "mr.dd %llx", mr.dd);
            if (mr.dd != NULL) {
                // existingDD freed in the below call
                sk_set_val_zero_copy(existingVal, dd_length_with_header_and_index(mr.dd), (void *)(mr.dd));
            }
        } else {
            SKVal    *pval;
            
            pval = sk_create_val();
            sk_set_val(pval, dd_length_with_header_and_index(ddwr->dirData), (void *)(ddwr->dirData) );
            requestGroup.insert( StrValMap::value_type(string(ddwr->dirName), pval));
        }
        dd_delete(&ddwr->dirData);
        
        srfsLog(LOG_INFO, "DirDataWriter ignoring %s", ddwr->dirName );
        
        if (ddw == NULL) {
            ddw = ddwr->ddw;
        } else {
            if (ddwr->ddw != ddw) {
                fatalError("Unexpected multiple DirDataWriter in ddw_process_dht_batch");
            }
        }
        ddwr_delete(&ddwr);
        requests[i] = NULL;
    }
    
    if (ddw == NULL) {
        fatalError("NULL ddw");
    }
    
    srfsLog(LOG_FINE, "DirDataWriter call mput");
    SKOperationState::SKOperationState opState = SKOperationState::INCOMPLETE;
    SKAsyncPut * pPut = NULL;
    try {
        pPut = ddw->ansp->put(&requestGroup);
        // FUTURE consider changing wait to (some form of) looping through results?
        pPut->waitForCompletion();

        opState = pPut->getState();
        if (srfsLogLevelMet(LOG_FINE)) {
            srfsLog(LOG_FINE, "DirDataWriter mput %s %d %d %d", SKFS_DIR_NS, requestGroup.size(), numRequests, opState );
        }
        if (opState == SKOperationState::FAILED){
            OpStateMap    *pOpMap = pPut->getOperationStateMap();
            for (i = 0; i < numRequests; i++) {
                SKOperationState::SKOperationState  iop;
                
                DirDataWriteRequest    *ddwr = (DirDataWriteRequest *)requests[i];
                
                if (ddwr != NULL) {
                    try {
                        iop = pOpMap->at(ddwr->dirName);
                    } catch(std::exception& emap) { 
                        iop = SKOperationState::FAILED;
                        srfsLog(LOG_INFO, "ddw std::map exception at %s:%d\n%s\n", __FILE__, __LINE__, emap.what()); 
                    }
                    if (iop) {
                        if (iop != SKOperationState::SUCCEEDED) {
                            // pPut->getFailureCause(ddwr->dirName);  // FUTURE: can SK get failure for individual keys
                            srfsLog(LOG_WARNING, "DirDataWriter mput state %s %d %d,  %s %d ", ddwr->dirName, 
                                    (requestGroup.at(ddwr->dirName))->m_len, iop,  __FILE__, __LINE__);
                        }
                    } else {
                        srfsLog(LOG_WARNING, "DirDataWriter mput state %s %d, %s %d", ddwr->dirName,
                                (requestGroup.at(ddwr->dirName))->m_len,  __FILE__, __LINE__);
                    }
                }
            }
            try {
                SKFailureCause::SKFailureCause cause = pPut->getFailureCause();
                srfsLog(LOG_WARNING, "ddw failed, cause %d", cause);
            } catch(SKClientException & e) { 
                srfsLog(LOG_WARNING, "ddw getFailureCause exception line %d\n%s\n", __LINE__, e.what()); 
            } catch(std::exception& e) { 
                srfsLog(LOG_WARNING, "ddw getFailureCause exception line %d\n%s\n", __LINE__, e.what()); 
            }
            delete pOpMap;
        }
    } catch (SKPutException &e) {
        const char    *cause;
        
        cause = e.what();
        if (cause == NULL || strstr(cause, "INVALID_VERSION") == NULL) {
            srfsLog(LOG_ERROR, "ddw mput dhtErr at %s:%d\n%s\n", __FILE__, __LINE__, cause);
            srfsLog(LOG_WARNING, " %s\n",  e.getDetailedFailureMessage().c_str());
        }
        /*
        Catching failed puts is deprecated for now in favor of the periodic reconciliation approach.
        {
            OpStateMap *pOpMap = pPut->getOperationStateMap();
            for (i = 0; i < numRequests; i++) {
                OpenDirWriteRequest    *ddwr = (OpenDirWriteRequest *)requests[i];
                SKOperationState::SKOperationState iop = pOpMap->at(ddwr->dirName);
                if (iop) {
                    if (iop != SKOperationState::SUCCEEDED) {
                        requests[i] = NULL;
                        {
                            SKVal *ppval = requestGroup.at(ddwr->dirName);
                            if (ppval == NULL || ppval->m_len == 0) {
                                srfsLog(LOG_WARNING, "ddw unexpected NULL %s %s\n", SKFS_DIR_NS, ddwr->dirName);
                            }
                            ppval->m_len = 0; 
                            sk_destroy_val(&ppval);
                        }
                        ddw_retry(ddw, ddwr);
                    }
                }
            }
        }
        */
    } catch (SKClientException &e) {
        srfsLog(LOG_ERROR, "ddw mput dhtErr at %s:%d\n%s\n", __FILE__, __LINE__, e.what());
    } catch (exception & e ){
        srfsLog(LOG_ERROR, "ddw mput dhtErr Exception %s at", e.what(), __FILE__, __LINE__);
        fatalError("Unexpected exception", __FILE__, __LINE__);
    }

    postWriteTimeMillis = curTimeMillis();
    
    for (i = 0; i < numRequests; i++) {
        DirDataWriteRequest    *ddwr = (DirDataWriteRequest *)requests[i];
        if (ddwr != NULL) {
            try {
                SKVal *ppval = requestGroup.at(ddwr->dirName);
                if (ppval == NULL || ppval->m_len == 0) {
                    srfsLog(LOG_WARNING, "ddw unexpected NULL %s %s\n", SKFS_DIR_NS, ddwr->dirName);
                }
                ppval->m_len = 0; 
                sk_destroy_val(&ppval);
                // unlike AttrReader (which uses ActiveOp to perform the deletion), we must delete the requests
            } catch (const std::out_of_range &oor) {
                srfsLog(LOG_WARNING, "Key not found: %s. Assuming it was a duplicate", ddwr->dirName);
            } catch (exception &e) {
                srfsLog(LOG_ERROR, "ddw mput dhtErr Exception %s at %s %d", e.what(), __FILE__, __LINE__);
                fatalError("Unexpected exception", __FILE__, __LINE__);
            }
            ddwr_delete(&ddwr);
        }
    }
    
    if (pPut) {
        pPut->close();
        delete pPut;
    }
    srfsLog(LOG_FINE, "out ddw_process_dht_batch");
}

/*

Retry logic is currently deprecated in favor of periodic reconciliation

static void ddw_retry(DirDataWriter *ddw, OpenDirWriteRequest *ddwr) {
    int    added;
    
    srfsLog(LOG_WARNING, "Queueing retry od %s", ddwr->dirName);
    if (ddwr->od->queuedForWrite) { // unsafe access as hint
        added = FALSE;
    } else {
        added = qp_add(ddw->retryQP, ddwr);
    }
    if (!added) {
        ddwr_delete(&ddwr);
        srfsLog(LOG_WARNING, "Failed retry addition %s %d", __FILE__, __LINE__);
    }
}

static int ddw_process_retry(OpenDirWriteRequest *ddwr) {
    int        okToAdd;
    int        added;
    OpenDir    *od;

    od = ddwr->od;
    okToAdd = od_set_queued_for_write(od, TRUE);
    if (okToAdd) {
        DirDataWriter    *ddw;
        
        srfsLog(LOG_WARNING, "Retrying od, updating %s", od->path);
        ddw = ddwr->openDirWriter;
        ddr_update_OpenDir(ddw->ddr, od);
        srfsLog(LOG_WARNING, "Update complete %s", od->path);
        added = qp_add(ddw->qp, ddwr);
        if (!added) {
            ddwr_delete(&ddwr);
            fatalError("Unexpected failed process retry addition", __FILE__, __LINE__);
        }
    } else {
        srfsLog(LOG_WARNING, "Ignoring already queued od %s", od->path);
        ddwr_delete(&ddwr);
    }
    return okToAdd;
}

static void ddw_process_retry_batch(void **requests, int numRequests, int curThreadIndex) {
    int    i;
    int    retryAttempted;

    srfsLog(LOG_WARNING, "in ddw_process_retry_batch %d", curThreadIndex);
    srfsLog(LOG_WARNING, "ddw_process_retry_batch sleeping");
    srfsLog(LOG_WARNING, "ddw_process_retry_batch retrying %d", numRequests);
    retryAttempted = FALSE;
    for (i = 0; i < numRequests; i++) {
        OpenDirWriteRequest    *ddwr;
        int    okToAdd;

        ddwr = (OpenDirWriteRequest *)requests[i];
        okToAdd = ddw_process_retry(ddwr);
        retryAttempted = retryAttempted || okToAdd;
    }
    if (retryAttempted) {
        sleep_random_millis(_minRetrySleepMillis, _maxRetrySleepMillis, &_retrySeed);
    }
    srfsLog(LOG_WARNING, "out ddw_process_retry_batch %d", curThreadIndex);
}
*/