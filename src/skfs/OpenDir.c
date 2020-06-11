// OpenDir.c

///////////////
// includes

#include "OpenDir.h"
#include "OpenDirUpdate.h"
#include "OpenDirWriter.h"
#include "ReconciliationSet.h"

#include <string.h>

#include <set>

////////////
// defines

#define OD_MIN_PREFETCH_INTERVAL_MILLIS    (1 * 60 * 1000)
#define OD_TRIGGER_INTERVAL_MILLIS    4
#define OD_RECONCILIATION_PERIOD_MILLIS (3 * 60 * 1000 + 1000)
#define OD_EXTRA_UPDATE_MARGIN_MILLIS 20

// update limits
#define OD_UL_MIN_MILLIS 10000
#define OD_UL_MAX_MILLIS (10 * 60 * 1000)
#define OD_UL_MILLIS_PER_ENTRY 10
#define OD_UL_MILLIS_PER_UPDATE 100

#define _OD_DEBUG_MERGE 0


///////////////////
// public globals

OpenDirWriter    *od_odw;
OpenDirWriter    *od_ddw;


//////////////////////
// private functions

static void od_merge_DirData_pendingUpdates(OpenDir *od);
static void od_clear_pending_updates(OpenDir *od);
static void od_remove_from_reconciliation(OpenDir *od);
static void od_set_server_update_DirData(OpenDir *od, DirData *su_dd);
static void od_setTimeToCurrentTime(OpenDir *od, uint64_t *timePtr);
static void od_setTimeIfGreater(OpenDir *od, uint64_t *timePtr, uint64_t newTime);
static uint64_t od_getTime(OpenDir *od, uint64_t *timePtr);
static uint64_t od_update_limit(OpenDir *od);
static bool od_reconciliation_outstanding(OpenDir *od);

///////////////
// implementation

OpenDir *od_new(const char *path, DirData *dirData) {
    OpenDir    *od;
    int    i;
    pthread_mutexattr_t mutexAttr;

    od = (OpenDir *)mem_alloc(1, sizeof(OpenDir));
    srfsLog(LOG_FINE, "od_new:\t%s\n", path);
    strncpy(od->path, path, SRFS_MAX_PATH_LENGTH);
    
    if (dirData != NULL) {
        od->dd = dd_dup(dirData);
    } else {
        od->dd = dd_new_empty();
    }
    
    od->mutex = &od->mutexInstance;
    pthread_mutexattr_init(&mutexAttr);
    pthread_mutexattr_settype(&mutexAttr, PTHREAD_MUTEX_RECURSIVE);    
    if (pthread_mutex_init(od->mutex, &mutexAttr) != 0) {
        fatalError("\n mutex init failed", __FILE__, __LINE__);
    }
    cv_init(&od->cvInstance, &od->cv);
    spinlock_init(&od->timeSpinlockInstance, &od->timeSpinlock);
    od->lastMutationMillis = 0;
    od->lastReconciliationTriggerMillis = 0;
    // Force an update
    od->lastUpdateMillis = 0;
    od->needsReconciliation = TRUE;
    rcst_add_to_reconciliation_set(od->path);
    od->su_dd = NULL;
    
    return od;
}

void od_delete(OpenDir **od) {
    if (od != NULL && *od != NULL) {
        int    i;
        
        if ((*od)->dd != NULL) {
            dd_delete(&(*od)->dd);
        }
        
        // Free pending updates
        for (i = 0; i < (*od)->numPendingUpdates; i++) {
            mem_free((void **)&(*od)->pendingUpdates[i].name);
        }
        if ((*od)->pendingUpdates != NULL) {
            mem_free((void **)&(*od)->pendingUpdates);
        }
        
        pthread_mutex_destroy((*od)->mutex);
        mem_free((void **)od);
    } else {
        fatalError("bad ptr in od_delete");
    }
}

uint64_t od_getLastUpdateMillis(OpenDir *od) {
    return od->lastUpdateMillis;
}

uint64_t od_getElapsedSinceLastUpdateMillis(OpenDir *od) {
    return curTimeMillis() - od->lastUpdateMillis;
}

uint64_t od_getLastWriteMillis(OpenDir *od) {
    return od->lastWriteMillis;
}

void od_setLastWriteMillis(OpenDir *od, uint64_t lastWriteMillis) {
    srfsLog(LOG_INFO, "setLastWriteMillis %s %lu", od->path, lastWriteMillis);
    pthread_mutex_lock(od->mutex);    
    od->lastWriteMillis = lastWriteMillis;
    pthread_cond_signal(od->cv);
    pthread_mutex_unlock(od->mutex);    
}    

void od_waitForWrite(OpenDir *od, uint64_t writeTimeMillis) {
    srfsLog(LOG_INFO, "Waiting for write %s %lu %lu", od->path, writeTimeMillis, od->lastWriteMillis);
    while (od->lastWriteMillis < writeTimeMillis) {
        pthread_cond_wait(od->cv, od->mutex);
    }
    srfsLog(LOG_INFO, "Wait for write complete %s %lu %lu", od->path, writeTimeMillis, od->lastWriteMillis);
}
    
void od_mark_deleted(OpenDir *od) {
    // FUTURE - for now we can't free data or reconciliation may go bad
    /*
    pthread_mutex_lock(od->mutex);
    dd_delete(&od->dd);
    od->dd = dd_new_empty();
    od_clear_pending_updates(od);
    pthread_mutex_unlock(od->mutex);
    */
}

// lock must be held
static void od_clear_pending_updates(OpenDir *od) {
    if (od->numPendingUpdates > 0) {
        int    i;
        
        for (i = 0; i < od->numPendingUpdates; i++) {
            mem_free((void **)&od->pendingUpdates[i].name);
        }
        if (od->pendingUpdates != NULL) {
            mem_free((void **)&od->pendingUpdates);
        }
        od->numPendingUpdates = 0;
    }
}

DirData *od_get_server_update_DirData(OpenDir *od) {
    DirData    *dd;
    
    pthread_mutex_lock(od->mutex);    
    if (od->su_dd != NULL) {
        dd = dd_dup(od->su_dd);
        srfsLog(LOG_INFO, "od_get_server_update_DirData od->su_dd %d", dd->numEntries);
    } else {
        dd = dd_process_updates(od->dd, od->pendingUpdates, od->numPendingUpdates);    
        srfsLog(LOG_INFO, "od_get_server_update_DirData od->dd %d", dd->numEntries);
    }
    pthread_mutex_unlock(od->mutex);    
    return dd;
}

// lock must be held
static void od_set_server_update_DirData(OpenDir *od, DirData *su_dd) {
    if (od->su_dd != NULL) {
        dd_delete(&od->su_dd);
    }
    od->su_dd = su_dd;
}

DirData *od_get_DirData(OpenDir *od, int clearPending) {
    DirData    *dd;
    
    pthread_mutex_lock(od->mutex);    
    
    dd = dd_process_updates(od->dd, od->pendingUpdates, od->numPendingUpdates);    
    if (clearPending) {
        if (od->dd) {
            dd_delete(&od->dd);
        }
        od->dd = dd;
        dd = dd_dup(dd);
        od_clear_pending_updates(od);
    }
    
    pthread_mutex_unlock(od->mutex);    
    return dd;
}

// lock must be held
static void od_merge_DirData_pendingUpdates(OpenDir *od) {
    DirData    *dd;
    
    dd = dd_process_updates(od->dd, od->pendingUpdates, od->numPendingUpdates);    
    if (od->dd) {
        dd_delete(&od->dd);
    }
    od->dd = dd;
    od_clear_pending_updates(od);
}

static void od_add_update(OpenDir *od, char *name, int type, uint64_t version) {
    int    i;
    int    existing;
    
    existing = FALSE;
    // lock
    pthread_mutex_lock(od->mutex);    
    //srfsLog(LOG_WARNING, "od_add_update %s %d %d", name, type, version);
    for (i = 0; i < od->numPendingUpdates; i++) {
        if (!strncmp(name, od->pendingUpdates[i].name, SRFS_MAX_PATH_LENGTH)) {
            existing = TRUE;
            if (version >= od->pendingUpdates[i].version) {
                srfsLog(LOG_FINE, "Modifying update %s to %d, %d", name, type, version);
                odu_modify(&od->pendingUpdates[i], type, version);
            } else {
                srfsLog(LOG_FINE, "Ignoring stale update %s %d, %d", name, type, version);
            }
            break;
        }
    }
    if (!existing) {
        // queue an update
        mem_realloc((void **)&od->pendingUpdates, od->numPendingUpdates, od->numPendingUpdates + 1, sizeof(OpenDirUpdate));
        odu_init(&od->pendingUpdates[od->numPendingUpdates], type, version, name);
        od->numPendingUpdates++;
        /*
        od_setTimeToCurrentTime(od, &od->lastMutationMillis);
        rcst_add_to_reconciliation_set(od->path); // notifies waiting update thread
        od->needsReconciliation = TRUE;
        */
    }
    od_setTimeToCurrentTime(od, &od->lastMutationMillis);
    rcst_add_to_reconciliation_set(od->path); // notifies waiting update thread
    od->needsReconciliation = TRUE;
    // unlock
    pthread_mutex_unlock(od->mutex);
}

void od_recordReconciliationTrigger(OpenDir *od) {
    od_setTimeToCurrentTime(od, &od->lastReconciliationTriggerMillis);
}

void od_recordReconciliationComplete(OpenDir *od) {
    od_setTimeToCurrentTime(od, &od->lastReconciliationCompleteMillis);
}

int od_needs_reconciliation(OpenDir *od) {
    uint64_t    lastMutationMillis;
    uint64_t    lastReconciliationTriggerMillis;
    
    lastMutationMillis = od_getTime(od, &od->lastMutationMillis);
    lastReconciliationTriggerMillis = od_getTime(od, &od->lastReconciliationTriggerMillis);
    if ((lastMutationMillis + OD_EXTRA_UPDATE_MARGIN_MILLIS) >= lastReconciliationTriggerMillis) {
        uint64_t    lastReconciliationCompleteMillis;
        
        lastReconciliationCompleteMillis = od_getTime(od, &od->lastReconciliationCompleteMillis);
        return (lastReconciliationTriggerMillis > lastReconciliationCompleteMillis) // needed as the type is unsigned
                || (lastReconciliationCompleteMillis - lastReconciliationTriggerMillis < od_update_limit(od));
        // NOTE above assumes that any reconciliation will take measurably > 1 ms
        return TRUE;
    } else {
        return FALSE;
    }
}

static uint64_t od_update_limit(OpenDir *od) {
    uint64_t ul;
    
    pthread_mutex_lock(od->mutex);
    ul = 0;
    if (od->dd != NULL) {
        ul += od->dd->numEntries * OD_UL_MILLIS_PER_ENTRY;
    }
    ul += od->numPendingUpdates * OD_UL_MILLIS_PER_UPDATE;
    ul = uint64_max(ul, OD_UL_MIN_MILLIS);
    ul = uint64_min(ul, OD_UL_MAX_MILLIS);
    pthread_mutex_unlock(od->mutex);
    return ul;
}

static void od_setTimeToCurrentTime(OpenDir *od, uint64_t *timePtr) {
    od_setTimeIfGreater(od, timePtr, curTimeMillis());
}

static void od_setTimeIfGreater(OpenDir *od, uint64_t *timePtr, uint64_t newTime) {
    pthread_spin_lock(od->timeSpinlock);
    if (newTime > *timePtr) {
        *timePtr = newTime;
    }
    pthread_spin_unlock(od->timeSpinlock);
}

static uint64_t od_getTime(OpenDir *od, uint64_t *timePtr) {
    uint64_t    timeVal;
    
    pthread_spin_lock(od->timeSpinlock);
    timeVal = *timePtr;
    pthread_spin_unlock(od->timeSpinlock);
    return timeVal;
}

void od_rm_entry(OpenDir *od, char *name, uint64_t version) {
    od_add_update(od, name, ODU_T_DELETION, version);
}

void od_add_entry(OpenDir *od, char *name, uint64_t version) {
    od_add_update(od, name, ODU_T_ADDITION, version);
}

void od_check_for_remove_from_reconciliation(OpenDir *od, uint64_t _curTimeMillis) {
    srfsLog(LOG_INFO, "%ld %ld %ld %d\t%s %d", _curTimeMillis, od->lastUpdateMillis, _curTimeMillis - od->lastUpdateMillis, OD_RECONCILIATION_PERIOD_MILLIS, __FILE__, __LINE__);
    if (!od_needs_reconciliation(od) && !od_reconciliation_outstanding(od) && (_curTimeMillis - od->lastUpdateMillis > OD_RECONCILIATION_PERIOD_MILLIS)) {
        od_remove_from_reconciliation(od);
    }
}

static bool od_reconciliation_outstanding(OpenDir *od) {
    uint64_t    lastReconciliationTriggerMillis;
    uint64_t    lastReconciliationCompleteMillis;
    
    lastReconciliationTriggerMillis = od_getTime(od, &od->lastReconciliationTriggerMillis);
    lastReconciliationCompleteMillis = od_getTime(od, &od->lastReconciliationCompleteMillis);
    return lastReconciliationTriggerMillis > lastReconciliationCompleteMillis;
}

static void od_remove_from_reconciliation(OpenDir *od) {
    rcst_remove_from_reconciliation_set(od->path);
    od->needsReconciliation = FALSE; 
    srfsLog(LOG_FINE, "Remove from rcst %s", od->path);
}

int od_add_DirData(OpenDir *od, DirData *dd, SKMetaData *metaData) {
    uint64_t    metaDataVersion;
    uint64_t    _curTimeMillis;
    int         deleteOnExit;
    int         kvsMissingLocalData;

    srfsLog(_OD_DEBUG_MERGE ? LOG_WARNING : LOG_INFO, "od_add_DirData %llx %llx", od, dd);
    kvsMissingLocalData = FALSE;
    _curTimeMillis = curTimeMillis();
    if (dd == NULL) {
        dd = dd_new_empty();
        deleteOnExit = TRUE;
        metaDataVersion = _curTimeMillis;
    } else {
        deleteOnExit = FALSE;
        metaDataVersion = metaData->getVersion();
    }
    pthread_mutex_lock(od->mutex);    
    srfsLog(_OD_DEBUG_MERGE ? LOG_WARNING : LOG_INFO, "od->ddVersion %ld\tmetaDataVersion %ld\tlastMergedVersion %ld", od->ddVersion, metaDataVersion, od->lastMergedVersion);
    //if (od->ddVersion < metaDataVersion && od->lastMergedVersion != metaDataVersion) {
    if (od->ddVersion <= metaDataVersion && od->lastMergedVersion != metaDataVersion) {
        MergeResult    mr;
        
        od_merge_DirData_pendingUpdates(od);
        mr = dd_merge(od->dd, dd);        
        srfsLog(_OD_DEBUG_MERGE ? LOG_WARNING : LOG_INFO, "mr.ddForUpdate %llx", mr.ddForUpdate);
        od_set_server_update_DirData(od, mr.ddForUpdate);

        if (mr.dd0NotIn1) {
            // make sure that we trigger a read to get the merged server version
            rcst_add_to_reconciliation_set(od->path);
            od->lastUpdateMillis = _curTimeMillis; // update time so that we don't leave the rcst
        }
        
        //od->lastMergedVersion = metaDataVersion; // Moved below for now
        // if kvs data had no new data
        if (!mr.dd1NotIn0) {
            od->lastMergedVersion = metaDataVersion; // Repurposing lastMergedVersion as last merged & verified version
            srfsLog(_OD_DEBUG_MERGE ? LOG_WARNING : LOG_INFO, "No updates from KVS data");
            // Note, mr.dd is always NULL in this case, no need to free
            //od->needsReconciliation = FALSE; 
        } else {
            // kvs store had new data
            dd_delete(&od->dd);
            od->dd = mr.dd;
            od->lastUpdateMillis = _curTimeMillis;
            od->ddVersion = metaDataVersion;
        }
        // if local data had entries not in the kvs, update kvs with merged result
        if (mr.dd0NotIn1) {
            srfsLog(_OD_DEBUG_MERGE ? LOG_WARNING : LOG_INFO, "Updating KVS version with local data");
            //odw_write_dir(od_odw, od->path, od);
            kvsMissingLocalData = TRUE; // new code does the write externally
        }
        // if local data and kvs data are an exact match
        if (!mr.dd1NotIn0 && !mr.dd0NotIn1) {
            // This logic is only valid for server-side reconciliation
            // Client-side has effectively been deprecated
            od_check_for_remove_from_reconciliation(od, _curTimeMillis);
        } else {
            od->needsReconciliation = TRUE;
        }
    } else {
        srfsLog(_OD_DEBUG_MERGE ? LOG_WARNING : LOG_INFO, "Ignoring stale or duplicate update %llx %ld", od, od->ddVersion);
        od_check_for_remove_from_reconciliation(od, _curTimeMillis);
    }
    pthread_mutex_unlock(od->mutex);    
    if (deleteOnExit) {
        dd_delete(&dd);
    }
    srfsLog(_OD_DEBUG_MERGE ? LOG_WARNING : LOG_INFO, "out od_add_DirData %llx %llx", od, dd);
    return kvsMissingLocalData;
}

int od_updates_pending(OpenDir *od) {
    return od->numPendingUpdates > 0;
}

int od_set_queued_for_write(OpenDir *od, int queuedForWrite) {
    int    markedByThisCall;
    
    pthread_mutex_lock(od->mutex);    
    if (od->queuedForWrite != queuedForWrite) {
        markedByThisCall = TRUE;
        od->queuedForWrite = queuedForWrite;
    } else {
        markedByThisCall = FALSE;
    }
    pthread_mutex_unlock(od->mutex);    
    return markedByThisCall;
}

void od_display(OpenDir *od, FILE *file) {
    int    i;
    
    pthread_mutex_lock(od->mutex);
    fprintf(file, "******************\n");
    dd_display(od->dd);
    fprintf(file, "\nnumPendingUpdates: %d\n", od->numPendingUpdates);
    for (i = 0; i < od->numPendingUpdates; i++) {
        fprintf(file, "\t\t%s\t%d\n", od->pendingUpdates[i].name, od->pendingUpdates[i].type);
    }
    fprintf(file, "******************\n");
    pthread_mutex_unlock(od->mutex);
}

int od_record_get_attr(OpenDir *od, char *child, uint64_t curTime) {
    /*
    int    doPrefetch;
    
    doPrefetch = (curTime - od->lastGetAttr <= OD_TRIGGER_INTERVAL_MILLIS) 
            && (curTime - od->lastPrefetch > OD_MIN_PREFETCH_INTERVAL_MILLIS);
    od->lastGetAttr = curTime;
    if (doPrefetch) {
        od->lastPrefetch = curTime;
    }
    return doPrefetch;
    */
    // FIXME - temp disable prefetching - seems to be memory corruption caused in the current prefetch code
    // resolve this as prefetch provides a critical speed boost
    return FALSE;
}
