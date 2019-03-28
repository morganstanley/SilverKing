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

#define WFT_ATTR_WRITE_MAX_ATTEMPTS	12


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
	WritableFileTable	*wft;
	int	i;

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
	int	i;
	
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

WritableFileReference *wft_create_new_file(WritableFileTable *wft, const char *name, mode_t mode, 
                                      FileAttr *fa, PartialBlockReader *pbr, int *retryFlag) {
	HashTableAndLock	*htl;
	WritableFile		*existingWF;
    WritableFileReference    *wf_userRef;
	
    wf_userRef = NULL;
	htl = wft_get_htl(wft, name);
	pthread_rwlock_wrlock(&htl->rwLock);
    existingWF = (WritableFile *)hashtable_search(htl->ht, (void *)name); 
	if (existingWF != NULL) {
		srfsLog(LOG_INFO, "Found existing wft entry %s", name);
        wf_userRef = NULL;
	} else {		
        WritableFile		*createdWF;
    
        // wf_new can issue remove ops; we must drop the wft lock 
        // to avoid blocking wft access while the op proceeds
        pthread_rwlock_unlock(&htl->rwLock);
		createdWF = wf_new(name, mode, htl, wft->aw, fa, pbr, retryFlag);
        if (createdWF == NULL) {
            // creation failed; no lock held; exit
            return NULL;
        } else {
            // We must now reacquire the lock and check to see if 
            // the wf was placed in while we didn't have the lock
            pthread_rwlock_wrlock(&htl->rwLock);
            existingWF = (WritableFile *)hashtable_search(htl->ht, (void *)name); 
            if (existingWF != NULL) {
                srfsLog(LOG_INFO, "Found existing wft entry on recheck %s", name);
                wf_delete(&createdWF); // delete directly as no refs exist
            } else {
                // No existingWF found on recheck; use the createdWF
                srfsLog(LOG_INFO, "Inserting new wft entry ht %llx name %s createdWF %llx", 
                                  htl->ht, name, createdWF);
                hashtable_insert(htl->ht, (void *)str_dup_no_dbg(name), createdWF);
                wf_userRef = wf_add_reference(createdWF, __FILE__, __LINE__);
            }
        }
	}
	pthread_rwlock_unlock(&htl->rwLock);
	return wf_userRef;
}

WritableFileReference *wft_get(WritableFileTable *wft, const char *name) {
	HashTableAndLock	*htl;
	WritableFile		*wf;
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
	HashTableAndLock	*htl;
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
	HashTableAndLock	*htl;
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
	HashTableAndLock	*htl;
	WritableFile		*wf;
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
int wft_delete_file(WritableFileTable *wft, const char *name, int deleteBlocks) {
    SKOperationState::SKOperationState	awResult;
    int result;    
    int getAttrResult;
    FileAttr _fa;
    FileAttr *fa;

    fa = &_fa;
    memset(fa, 0, sizeof(FileAttr));
    getAttrResult = ar_get_attr(wft->ar, (char *)name, fa);
    awResult = aw_write_attr_direct(wft->aw, name, fa_get_deletion_fa(), wft->ac, WFT_ATTR_WRITE_MAX_ATTEMPTS);
    srfsLog(LOG_FINE, "wft_delete_file %s aw_write result %d\n", name, awResult);
    if (awResult != SKOperationState::SUCCEEDED) {
        result = EIO;
    } else {
        if (!getAttrResult) {
            if (deleteBlocks) {
                srfsLog(LOG_FINE, "wft_delete_file %s invalidating %d blocks\n", name, fa->stat.st_blocks);
                fbw_invalidate_file_blocks(wft->fbw, &fa->fid, fa->stat.st_blocks);
            }
            result = 0;
        } else {
            srfsLog(LOG_FINE, "wft_delete_file %s failed to retrieve FileAttr; %d", name, getAttrResult);
            result = EIO;
        }
    }
    return result;
}
