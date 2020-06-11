// WritableFileTable.c

/////////////
// includes

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include  "HashTableAndLock.h"
#include "WritableFileTable.h"
#include "Util.h"


////////////////////
// private defines

#define WFT_ATTR_WRITE_MAX_ATTEMPTS    12
#define WFT_DELETION_OP_LOCK_SECONDS    60


///////////////////////
// private prototypes

static HashTableAndLock *wft_get_htl(WritableFileTable *wft, const char *path);


////////////////////
// private members

//static int _wftHashSize = 1024;
// use larger table until ht expand is fixed
static int _wftHashSize = 4 * 1024;


///////////////////
// implementation

WritableFileTable *wft_new(const char *name, AttrWriter *aw, AttrCache *ac, AttrReader *ar, FileBlockWriter *fbw) {
    WritableFileTable    *wft;
    int    i;

    wft = (WritableFileTable *)mem_alloc(1, sizeof(WritableFileTable));
    srfsLog(LOG_FINE, "wft_new:\t%s\n", name);
    wft->name = name;
    wft->aw = aw;
    wft->ac = ac;
    wft->ar = ar;
    wft->fbw = fbw;
    for (i = 0; i < WFT_NUM_HT; i++) {
        wft->htl[i].ht = create_hashtable(_wftHashSize, (unsigned int (*)(void *))stringHash, (int(*)(void *, void *))strcmp);
        pthread_rwlock_init(&wft->htl[i].rwLock, 0); 
    }
    return wft;
}

void wft_delete(WritableFileTable **wft) {
    int    i;
    
    if (wft != NULL && *wft != NULL) {
        for (i = 0; i < WFT_NUM_HT; i++) {
            // FUTURE - delete hashtable and entries
            // all current use cases never call this
            //delete_hashtable((*wft)->htl[i].ht);
            pthread_rwlock_destroy(&(*wft)->htl[i].rwLock);
        }
        mem_free((void **)wft);
    } else {
        fatalError("bad ptr in wft_delete");
    }
}

static HashTableAndLock *wft_get_htl(WritableFileTable *wft, const char *path) {
    srfsLog(LOG_FINE, "wft_get_htl %llx %d %llx", wft, stringHash((void *)path) % WFT_NUM_HT, &wft->htl[stringHash((void *)path) % WFT_NUM_HT]);
    return &(wft->htl[stringHash((void *)path) % WFT_NUM_HT]);
}

WFT_WFCreationResult wft_create_new_file(WritableFileTable *wft, const char *name, mode_t mode, 
                                      FileAttr *fa, int64_t createdVersion, PartialBlockReader *pbr, int *retryFlag) {
    HashTableAndLock    *htl;
    WritableFile        *existingWF;
    WritableFileReference    *wf_userRef;
    WFT_WFCreationResult    wft_wfcr;
    
    wft_wfcr.wfr = NULL;
    wf_userRef = NULL;
    htl = wft_get_htl(wft, name);
    pthread_rwlock_wrlock(&htl->rwLock);
    existingWF = (WritableFile *)hashtable_search(htl->ht, (void *)name); 
    if (existingWF != NULL) {
        srfsLog(LOG_INFO, "Found existing wft entry %s", name);
        wf_userRef = NULL;
        wft_wfcr.createdVersion = 0;
    } else {        
        WFCreationResult  wfcr;
    
        // wf_new can issue remove ops; we must drop the wft lock 
        // to avoid blocking wft access while the op proceeds
        pthread_rwlock_unlock(&htl->rwLock);
        wfcr = wf_new(name, mode, htl, wft->aw, fa, createdVersion, pbr, retryFlag);
        wft_wfcr.createdVersion = wfcr.createdVersion;
        if (wfcr.wf == NULL) {
            // creation failed; no lock held; exit
            wft_wfcr.wfr = NULL;
            wft_wfcr.createdVersion = 0;
            return wft_wfcr;
        } else {
            // We must now reacquire the lock and check to see if 
            // the wf was placed in while we didn't have the lock
            pthread_rwlock_wrlock(&htl->rwLock);
            existingWF = (WritableFile *)hashtable_search(htl->ht, (void *)name); 
            if (existingWF != NULL) {
                srfsLog(LOG_INFO, "Found existing wft entry on recheck %s", name);
                wf_delete(&wfcr.wf); // delete directly as no refs exist
            } else {
                // No existingWF found on recheck; use the wfcr.wf
                srfsLog(LOG_INFO, "Inserting new wft entry ht %llx name %s wfcr.wf %llx", 
                                  htl->ht, name, wfcr.wf);
                hashtable_insert(htl->ht, (void *)str_dup_no_dbg(name), wfcr.wf);
                wf_userRef = wf_add_reference(wfcr.wf, __FILE__, __LINE__);
            }
        }
    }
    pthread_rwlock_unlock(&htl->rwLock);
    wft_wfcr.wfr = wf_userRef;
    return wft_wfcr;
}

WritableFileReference *wft_get(WritableFileTable *wft, const char *name) {
    HashTableAndLock    *htl;
    WritableFile        *wf;
    WritableFileReference    *wf_userRef;
    
    wf_userRef = NULL;
    htl = wft_get_htl(wft, name);
    pthread_rwlock_rdlock(&htl->rwLock);
    wf = (WritableFile *)hashtable_search(htl->ht, (void *)name); 
    if (wf != NULL) {
        wf_userRef = wf_add_reference(wf, __FILE__, __LINE__);
    }
    pthread_rwlock_unlock(&htl->rwLock);
    srfsLog(LOG_INFO, "wft_get %llx %llx %llx %s returning %llx", wft, htl, htl->ht, name, wf);
    return wf_userRef;
}

int wft_contains(WritableFileTable *wft, const char *name) {
    HashTableAndLock    *htl;
    WritableFile        *wf;
    
    htl = wft_get_htl(wft, name);
    pthread_rwlock_rdlock(&htl->rwLock);
    wf = (WritableFile *)hashtable_search(htl->ht, (void *)name); 
    pthread_rwlock_unlock(&htl->rwLock);
    return wf != NULL;
}

/*
// Only called by wfr_delete_ref()
// moved into wf_check_for_close()
int wft_check_file_for_removal(WritableFileTable *wft, WritableFile *wf) {
    HashTableAndLock    *htl;
    WritableFile        *wf;
    int rc;
    
    htl = wft_get_htl(wft, name);
    
    pthread_rwlock_rdlock(&htl->rwLock);
    wf = (WritableFile *)hashtable_search(htl->ht, (void *)name); 
    pthread_rwlock_unlock(&htl->rwLock);
    
    if (wf != NULL && wf_has_references(wf)) {
        pthread_rwlock_wrlock(&htl->rwLock);
        if (wf != NULL) {
            rc = wf_check_for_close(wf, aw, fbw, ac);
        } else {
            rc = 0; // ENOENT??
        }
        pthread_rwlock_unlock(&htl->rwLock);
    } else {
        return 0; // ENOENT sometimes??
    }
    return rc;
}
*/

/*
WritableFileReference *wft_remove(WritableFileTable *wft, const char *name) {
    HashTableAndLock    *htl;
    WritableFile        *wf;
    WritableFileReference    *wf_tableRef;
    
    srfsLog(LOG_INFO, "wft_remove %llx %s", wft, name);
    htl = wft_get_htl(wft, name);
    pthread_rwlock_wrlock(&htl->rwLock);
    wf_tableRef = (WritableFileReference *)hashtable_remove(htl->ht, (void *)name); 
    pthread_rwlock_unlock(&htl->rwLock);
    return wf_tableRef;
}
*/

// FUTURE - Consider moving this out of wft. No wft is currently used,
// but wft contains locks for controlling file creation.
int wft_delete_file(WritableFileTable *wft, const char *name, OpenDirTable *odt, int deleteBlocks) {
    AWWriteResult    awResult;
    int result;    
    FileAttr _fa;
    FileAttr *fa;
    
    ARReadAttrDirectResult  arReadAttrDirectResult;
    SKFailureCause::SKFailureCause  skFailureCause;

    skFailureCause = SKFailureCause::ERROR;
    fa = &_fa;
    memset(fa, 0, sizeof(FileAttr));
    
    arReadAttrDirectResult = ar_read_attr_direct(wft->ar, name);
    if (arReadAttrDirectResult.opState != SKOperationState::SUCCEEDED) {
        srfsLog(LOG_WARNING, "arReadAttrDirectResult.opState != SKOperationState::SUCCEEDED  %d %d  %s %d", 
                    arReadAttrDirectResult.opState, arReadAttrDirectResult.failureCause,
                    __FILE__, __LINE__);
        if (arReadAttrDirectResult.failureCause == SKFailureCause::NO_SUCH_VALUE) {
            result = -ENOENT;
        } else {
            result = -EIO;
        }
    } else {
        if (arReadAttrDirectResult.failureCause == SKFailureCause::NO_SUCH_VALUE) {
            result = -ENOENT;
        } else {
            srfsLog(LOG_FINE, "arReadAttrDirectResult.fa %llx", arReadAttrDirectResult.fa);
            if (arReadAttrDirectResult.fa == NULL) {
                result = -ENOENT;
            } else {
                memcpy(fa, arReadAttrDirectResult.fa, sizeof(FileAttr));
                
                int64_t lockMillisRemaining;

                lockMillisRemaining = arReadAttrDirectResult.metaData->getLockMillisRemaining();
                if (lockMillisRemaining > 0) {
                    if (lockMillisRemaining < SKFS_LOCK_CLOCK_SKEW_TOLERANCE_MILLIS) {
                        usleep(lockMillisRemaining * 1000);
                    } else {
                        srfsLog(LOG_WARNING, "wft_delete_file failed. %s lockMillisRemaining %d", name, lockMillisRemaining);
                        ar_delete_direct_read_result_contents(&arReadAttrDirectResult);
                        return -ENOLCK;
                    }
                }
                
                // In present approach, we don't need to hold a lock during directory update as the attribute is definitive. 
                // If the attribute write succeeds, then we must update the directory to match the attribute - using the
                // stored attribute version.
                awResult = aw_write_attr_direct(wft->aw, name, fa_get_deletion_fa(), wft->ac, WFT_ATTR_WRITE_MAX_ATTEMPTS,
                                                &skFailureCause, arReadAttrDirectResult.metaData->getVersion(), 0);
                if (awResult.operationState != SKOperationState::SUCCEEDED) {
                    srfsLog(LOG_WARNING, "awResult.operationState != SKOperationState::SUCCEEDED  %d %d  %s %d", awResult.operationState, skFailureCause, __FILE__, __LINE__);
                    // Handle failure
                    if (skFailureCause == SKFailureCause::INVALID_VERSION) {
                        result = -ENOLCK;
                    } else if (skFailureCause == SKFailureCause::LOCKED) {
                        result = -ENOLCK;
                    } else {
                        result = -EIO;
                    }
                } else {
                    // Remove from the parent directory
                    if (odt_rm_entry_from_parent_dir(odt, (char *)name, awResult.storedVersion)) {
                        // Couldn't remove the entry in the parent. Allow the lock to timeout (rather than immediately unlocking)
                        // FUTURE - consider attempting to backout the deletion
                        srfsLog(LOG_WARNING, "Couldn't rm entry in parent for %s  %s %d", name, __FILE__, __LINE__);
                        result = -EIO;
                    } else {
                        result = 0;
                    }
                }
                
                
                /*
                Deprecated. Remove when new code is stable.
                // Write lock the attr so that we can both delete the attribute and remove the directory entry
                awResult = aw_write_attr_direct(wft->aw, name, fa_get_deletion_fa(), wft->ac, WFT_ATTR_WRITE_MAX_ATTEMPTS,
                                                &skFailureCause, arReadAttrDirectResult.metaData->getVersion(), WFT_DELETION_OP_LOCK_SECONDS);
                if (awResult.operationState != SKOperationState::SUCCEEDED) {
                    srfsLog(LOG_WARNING, "awResult.operationState != SKOperationState::SUCCEEDED  %d %d  %s %d", awResult.operationState, skFailureCause, __FILE__, __LINE__);
                    // Handle failure
                    if (skFailureCause == SKFailureCause::INVALID_VERSION) {
                        result = -ENOLCK;
                    } else if (skFailureCause == SKFailureCause::LOCKED) {
                        result = -ENOLCK;
                    } else {
                        result = -EIO;
                    }
                } else {
                    // We have a lock on the attribute. Remove from the parent directory
                    if (odt_rm_entry_from_parent_dir(odt, (char *)name, awResult.storedVersion)) {
                        // Couldn't remove the entry in the parent. Allow the lock to timeout (rather than immediately unlocking)
                        // FUTURE - consider attempting to backout the deletion
                        srfsLog(LOG_WARNING, "Couldn't rm entry in parent for %s  %s %d", name, __FILE__, __LINE__);
                        result = -EIO;
                    } else {
                        // Entry in parent removed.
                        // Now write the deleted attribute
                        // Not specifying required previous version as we already have a lock.
                        // (Getting the actual previous version would entail another attribute fetch.)
                        awResult = aw_write_attr_direct(wft->aw, name, fa_get_deletion_fa(), wft->ac, WFT_ATTR_WRITE_MAX_ATTEMPTS, &skFailureCause, AW_NO_REQUIRED_PREV_VERSION, 0);
                        srfsLog(LOG_FINE, "wft_delete_file %s aw_write result %d\n", name, awResult.operationState);
                        if (awResult.operationState != SKOperationState::SUCCEEDED) {
                            srfsLog(LOG_WARNING, "awResult.operationState != SKOperationState::SUCCEEDED  %d %d  %s %d", awResult.operationState, skFailureCause, __FILE__, __LINE__);
                            result = -EIO;
                        } else {
                            if (deleteBlocks) {
                                srfsLog(LOG_FINE, "wft_delete_file %s invalidating %d blocks\n", name, fa->stat.st_blocks);
                                fbw_invalidate_file_blocks(wft->fbw, &fa->fid, fa->stat.st_blocks);
                            }
                            result = 0;
                        }
                    }
                }
                */
            }
        }        
    }
    
    ar_delete_direct_read_result_contents(&arReadAttrDirectResult);
    return result;
}
