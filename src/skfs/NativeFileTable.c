// NativeFileTable.c

/////////////
// includes

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include  "HashTableAndLock.h"
#include "NativeFile.h"
#include "NativeFileReference.h"
#include "NativeFileTable.h"
#include "Util.h"

///////////////////////
// private prototypes

static HashTableAndLock *nft_get_htl(NativeFileTable *nft, const char *path);


////////////////////
// private members

//static int _nftHashSize = 1024;
// use larger table until ht expand is fixed
static int _nftHashSize = 4 * 1024;


///////////////////
// implementation

NativeFileTable *nft_new(const char *name) {
	NativeFileTable	*nft;
	int	i;

	nft = (NativeFileTable *)mem_alloc(1, sizeof(NativeFileTable));
    srfsLog(LOG_WARNING, "nft_new:\t%s\tNFT_REF_TABLE_SIZE %d\n", name, NFT_REF_TABLE_SIZE);
    nft->name = name;
	for (i = 0; i < NFT_NUM_HT; i++) {
		nft->htl[i].ht = create_hashtable(_nftHashSize, (unsigned int (*)(void *))stringHash, (int(*)(void *, void *))strcmp);
		pthread_rwlock_init(&nft->htl[i].rwLock, 0); 
	}
	pthread_spin_init(&nft->tableRefsLock, 0);
	return nft;
}

void nft_delete(NativeFileTable **nft) {
	int	i;
	
	if (nft != NULL && *nft != NULL) {
		for (i = 0; i < NFT_NUM_HT; i++) {
			// FUTURE - delete hashtable and entries
			// all current use cases never call this
			//delete_hashtable((*nft)->htl[i].ht);
			pthread_rwlock_destroy(&(*nft)->htl[i].rwLock);
		}
		mem_free((void **)nft);
		pthread_spin_destroy(&(*nft)->tableRefsLock);
	} else {
		fatalError("bad ptr in nft_delete");
	}
}

NativeFileReference *nft_open(NativeFileTable *nft, const char *name) {
	HashTableAndLock	*htl;
	NativeFile       		*existingNF;
    NativeFileReference    *nf_userRef;
	
    nf_userRef = NULL;
	htl = nft_get_htl(nft, name);
	pthread_rwlock_rdlock(&htl->rwLock);
    existingNF = (NativeFile *)hashtable_search(htl->ht, (void *)name); 
	if (existingNF != NULL) {
		srfsLog(LOG_INFO, "Found existing nft entry %s", name);
        nf_userRef = nf_add_reference(existingNF, __FILE__, __LINE__);
	} else {		
        int fd;
    
        // drop nft lock while we open the file
        // to avoid blocking nft access while the open proceeds
        pthread_rwlock_unlock(&htl->rwLock);
        fd = open(name, O_RDONLY | O_NOFOLLOW);
        if (fd < 0) {
            // open failed; no lock held; exit
            return NULL;
        } else {
            // We must now reacquire the lock and check to see if 
            // the nf was placed in while we didn't have the lock
            pthread_rwlock_wrlock(&htl->rwLock);
            existingNF = (NativeFile *)hashtable_search(htl->ht, (void *)name); 
            if (existingNF != 0) { // 0 is never valid for files that we're interested in
                srfsLog(LOG_INFO, "Found existing nft entry on recheck %s", name);
                close(fd);
                nf_userRef = nf_add_reference(existingNF, __FILE__, __LINE__);
            } else {
                NativeFile *nf;
                NativeFileReference    *nf_tableRef;
                
                srfsLog(LOG_INFO, "Adding ref %s", name);
                // No existingNF found on recheck; use the new fd
                nf = nf_new(name, fd, htl);
                srfsLog(LOG_INFO, "Inserting new nft entry ht %llx name %s createdWF %llx", 
                                  htl->ht, name, nf);
                hashtable_insert(htl->ht, (void *)str_dup_no_dbg(name), nf);
                nf_userRef = nf_add_reference(nf, __FILE__, __LINE__);
                nf_tableRef = nf_add_reference(nf, __FILE__, __LINE__);
                pthread_spin_lock(&nft->tableRefsLock);
                if (nft->tableRefs[nft->nextRefIndex] != NULL) { // simplistic round robin deletion
                    //srfsLog(LOG_FINE, "Removing ref at %d", nft->nextRefIndex);
                    nfr_delete(&nft->tableRefs[nft->nextRefIndex], TRUE);
                    nft->tableRefs[nft->nextRefIndex] = NULL;
                }
                nft->tableRefs[nft->nextRefIndex] = nf_tableRef;
                nft->nextRefIndex = (nft->nextRefIndex + 1) % NFT_REF_TABLE_SIZE;
                pthread_spin_unlock(&nft->tableRefsLock);
            }
        }
	}
	pthread_rwlock_unlock(&htl->rwLock);
	return nf_userRef;
}

static HashTableAndLock *nft_get_htl(NativeFileTable *nft, const char *path) {
	srfsLog(LOG_FINE, "nft_get_htl %llx %d %llx", nft, stringHash((void *)path) % NFT_NUM_HT, &nft->htl[stringHash((void *)path) % NFT_NUM_HT]);
	return &(nft->htl[stringHash((void *)path) % NFT_NUM_HT]);
}
