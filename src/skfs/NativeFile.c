// NativeFile.c

/////////////
// includes

#include <errno.h>
#include <fuse.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "NativeFile.h"
#include "Util.h"


////////////////////
// private defines

#define NF_MAGIC	0xabfa


///////////////////////
// private prototypes

static int _nf_find_empty_ref(NativeFile *nf);
static int _nf_has_references(NativeFile *nf);
static int nf_close(NativeFile *nf);

///////////////////
// implementation

NativeFile *nf_new(const char *path, int fd, HashTableAndLock *htl) {
	NativeFile	*nf;
	pthread_mutexattr_t mutexAttr;
    
	nf = (NativeFile *)mem_alloc(1, sizeof(NativeFile));
	if (srfsLogLevelMet(LOG_FINE)) {
		srfsLog(LOG_FINE, "nf_new:\t%s %llx", path, nf);
	}
	
	nf->magic = NF_MAGIC;
    nf->path = str_dup(path);
    nf->fd = fd;
    nf->htl = htl;
    
	pthread_mutexattr_init(&mutexAttr);
	pthread_mutexattr_settype(&mutexAttr, PTHREAD_MUTEX_RECURSIVE);	
    pthread_mutex_init(&nf->lock, &mutexAttr); 

	return nf;
}


void nf_delete(NativeFile **nf) {
	if (nf != NULL && *nf != NULL) {
        srfsLog(LOG_FINE, "nf_delete %llx %llx", nf, *nf);
		(*nf)->magic = 0;
        mem_free((void **)&(*nf)->path);
		pthread_mutex_destroy(&(*nf)->lock);
		mem_free((void **)nf);
	} else {
		fatalError("bad ptr in nf_delete");
	}
}

int nf_get_fd(NativeFile *nf) {
    return nf != NULL ? nf->fd : -1;
}

NativeFileReference *nf_add_reference(NativeFile *nf, char *file, int line) {
    return nfr_new(nf, file, line);
}


// called only from nf_check_for_close()
static int nf_close(NativeFile *nf) {
	int		result;

	result = 0;
	srfsLog(LOG_FINE, "in nf_close %s", nf->path);
    pthread_mutex_lock(&nf->lock);    
    
    if (nf->fd >= 0) {
        result = close(nf->fd);
        nf->fd = -1;
    }
    
    pthread_mutex_unlock(&nf->lock);
	srfsLog(LOG_FINE, "leaving nf_close %s", nf->path);
	nf_delete(&nf);
	srfsLog(LOG_FINE, "out nf_close");
    return result;
}


//////////////////////////////
// reference code

int nf_create_ref(NativeFile *nf) {
	int	ref;

	ref = -1;
	pthread_mutex_lock(&nf->lock);
    
    if (nf->referentState.nextRef >= NFR_RECYCLE_THRESHOLD) {
        ref = _nf_find_empty_ref(nf);
    }
    if (ref >= 0) {
        nf->referentState.refStatus[ref] = NFR_Created;
    } else {
        if (nf->referentState.nextRef < NFR_MAX_REFS) {
            ref = nf->referentState.nextRef++;
            if (nf->referentState.refStatus[ref] != NFR_Invalid) {
                fatalError("nf->referentState.refStatus[ref] != NFR_Invalid", __FILE__, __LINE__);
            }
            nf->referentState.refStatus[ref] = NFR_Created;
        } else {
            fatalError("NFR_MAX_REFS exceeded", __FILE__, __LINE__);
        }
    }
    
	pthread_mutex_unlock(&nf->lock);
	return ref;
}

// lock must be held
static int _nf_find_empty_ref(NativeFile *nf) {
	int	i;

    for (i = 0; i < nf->referentState.nextRef; i++) {
        if (nf->referentState.refStatus[i] != NFR_Created) {
            return i;
        }
    }
    return -1;
}

// lock must be held
static int _nf_has_references(NativeFile *nf) {
	int	noReferences;
	int	i;

	noReferences = TRUE;
    for (i = 0; i < nf->referentState.nextRef; i++) {
        if (nf->referentState.refStatus[i] != NFR_Destroyed) {
            noReferences = FALSE;
            break;
        }
    }
    return !noReferences;
}

// Only called by nf_delete_ref()
// file and table locks must be held
static int nf_check_for_close(NativeFile *nf, int unlockTable) {
    int hasReferences;
    int rc;
    
    hasReferences = _nf_has_references(nf);
    if (!hasReferences) {
        NativeFile *_nf;
        
        nf_sanity_check(nf);
        _nf = (NativeFile *)hashtable_remove(nf->htl->ht, (void *)nf->path); 
        if (_nf != nf) {
            srfsLog(LOG_ERROR, "nf->htl->ht %llx nf->path %s _nf %llx nf %llx", 
                                nf->htl->ht, nf->path, _nf, nf);
            fatalError("_nf != nf", __FILE__, __LINE__);
        }
        // nf_flush() and nf_close() may cause remote I/O. We must drop the table lock
        // (must drop before releasing nf lock; release order inversion is ok)
        if (unlockTable) {
            pthread_rwlock_unlock(&nf->htl->rwLock);
        }
        // we removed nf from the table, so we no longer need a lock
        // hold it to here just to meet the documented lock contract of _nf_flush()
        pthread_mutex_unlock(&nf->lock);
        rc = nf_close(nf);
    } else {
        pthread_mutex_unlock(&nf->lock);
        if (unlockTable) {
            pthread_rwlock_unlock(&nf->htl->rwLock);
        }
        rc = 0;
    }
    return rc;
}

// Only called by nfr_delete
int nf_delete_ref(NativeFile *nf, int ref, int tableLockedExternally) {
    int hasReferences;
    HashTableAndLock    *htl;
    int rc;

    htl = nf->htl;
    if (!tableLockedExternally) {
        pthread_rwlock_wrlock(&htl->rwLock);    
    }
	pthread_mutex_lock(&nf->lock);
	if (ref >= nf->referentState.nextRef) {
		fatalError("ref >= nf->referentState.nextRef", __FILE__, __LINE__);
	}
	if (nf->referentState.refStatus[ref] != NFR_Created) {
		fatalError("nf->referentState.refStatus[ref] != NFR_Created", __FILE__, __LINE__);
	}
	nf->referentState.refStatus[ref] = NFR_Destroyed;
    
    // Before locking the table partition [in nf_check_for_close()], 
    // check the file for references
    hasReferences = _nf_has_references(nf);
    if (!hasReferences) {
        // locks are taken care of in below function
        rc = nf_check_for_close(nf, !tableLockedExternally);
    } else {
        pthread_mutex_unlock(&nf->lock);
        if (!tableLockedExternally) {
            pthread_rwlock_unlock(&htl->rwLock);
        }
        rc = 0;
    }
    return rc;
}

void nf_sanity_check(NativeFile *nf) {
    if (nf == NULL) {
        fatalError("Unexpected null nf", __FILE__, __LINE__);
    }
    if (nf->magic != NF_MAGIC) {
        srfsLog(LOG_ERROR, "nf %llx nf->magic %x NF_MAGIC %x", nf, nf->magic, NF_MAGIC);
        fatalError("Unexpected nf->magic != NF_MAGIC", __FILE__, __LINE__);
    }
}
