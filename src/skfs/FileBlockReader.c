// FileBlockReader.c

/////////////
// includes

#include "ActiveOp.h"
#include "ActiveOpRef.h"
#include "FileBlockID.h"
#include "FileBlockReader.h"
#include "FileBlockReadRequest.h"
#include "SRFSConstants.h"
#include "Util.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <zlib.h>
#include <exception>
using std::exception;


////////////////////
// private defines

#define _FBR_ERR_TIMEOUT_MILLIS (10 * 1000)

// For prefetch operations, there is no waiting thread. Hence,
// if an operation is reaped from the cache before the actual remote
// operation has completed, there is no significant adverse impact.
#define _FBR_PREFETCH_OP_TIMEOUT_MILLIS (5 * 60 * 1000)
// For normal read operations, however, there should be a waiting
// thread. Hence, we don't really want to reap the operation.
// In this case, this timeout acts as a fail-safe. In case, due to
// some bug, an operation is stranded, we allow it to be reaped.
#define _FBR_READ_OP_TIMEOUT_MILLIS (15 * 60 * 1000)


///////////////////////
// private prototypes

static void fbr_process_dht_batch(void **requests, int numRequests, int curThreadIndex);
static void fbr_process_nfs_request(void *_requestOp, int curThreadIndex);


/////////////////
// private data

static struct stat ENOENT_stat;
static int maxZeroReadRetries = 6;
static int bufPadding = 2048;


///////////////////
// implementation

FileBlockReader *fbr_new(FileIDToPathMap *f2p, 
						 FileBlockWriter *fbwCompress, FileBlockWriter *fbwRaw, 
						 SRFSDHT *sd, 
						 ResponseTimeStats *rtsDHT, ResponseTimeStats *rtsNFS,
						 FileBlockCache *fbc) {
	FileBlockReader *fbr;

	fbr = (FileBlockReader*)mem_alloc(1, sizeof(FileBlockReader));
	fbr->f2p = f2p;
	fbr->fbwCompress = fbwCompress;
	fbr->fbwRaw = fbwRaw;
    fbr->nft = nft_new("NativeFileTable");
	fbr->sd = sd;
    
	fbr->fileBlockCache = fbc;
	fbr->nfsFileBlockQueueProcessor = qp_new(fbr_process_nfs_request, __FILE__, __LINE__, FBR_NFS_QUEUE_SIZE, ABQ_FULL_BLOCK, FBR_NFS_THREADS);
	fbr->dhtFileBlockQueueProcessor = qp_new_batch_processor(fbr_process_dht_batch, __FILE__, __LINE__, 
											FBR_DHT_QUEUE_SIZE, ABQ_FULL_DROP, FBR_DHT_THREADS, FBR_MAX_BATCH_SIZE);
	fbr->rtsDHT = rtsDHT;
	fbr->rtsNFS = rtsNFS;
	fbr->rs = rs_new();
	fbr->compressedPaths = pg_new("compressedPaths", FALSE);
	fbr->noFBWPaths = pg_new("noFBWPaths", FALSE);
    try {
		int	i;
		
		for (i = 0; i < FBR_DHT_THREADS; i++) {
			SKNamespace	*ns;
			SKNamespacePerspectiveOptions *nspOptions;
			
			fbr->pSession[i] = sd_new_session(fbr->sd);
			ns = fbr->pSession[i]->getNamespace(SKFS_FB_NS);
			nspOptions = ns->getDefaultNSPOptions();
			fbr->ansp[i] = ns->openAsyncPerspective(nspOptions);
			delete ns;
		}
    } catch(std::exception &ex) {
		srfsLog(LOG_ERROR, "fbr_new exception opening namespace %s: what: %s\n", SKFS_FB_NS, ex.what());
        fatalError(" exception in fbr_new openAsyncNamespacePerspective", __FILE__, __LINE__ );
    } catch(...) {
        fatalError("unknown exception in fbr_new openAsyncNamespacePerspective", __FILE__, __LINE__ );
    }
	pthread_spin_init(&fbr->statLock, 0);
	return fbr;
}

void fbr_delete(FileBlockReader **fbr) {
	if (fbr != NULL && *fbr != NULL) {
		(*fbr)->nfsFileBlockQueueProcessor->running = FALSE;
		(*fbr)->dhtFileBlockQueueProcessor->running = FALSE;
		try {
			int	i;
			
			for (i = 0; i < FBR_DHT_THREADS; i++) {
				(*fbr)->ansp[i]->waitForActiveOps();
				(*fbr)->ansp[i]->close();
				delete (*fbr)->ansp[i];
			}
		} catch (std::exception & ex) {
			srfsLog(LOG_ERROR, "exception in fbr_delete: ns %s what: %s\n", SKFS_FB_NS, ex.what());
			fatalError("exception in fbr_delete", __FILE__, __LINE__ );
		}
		fbc_delete(&(*fbr)->fileBlockCache);

		for(int i=0; i<(*fbr)->dhtFileBlockQueueProcessor->numThreads; i++) {
			int added = qp_add((*fbr)->dhtFileBlockQueueProcessor, NULL);
			if (!added) srfsLog(LOG_ERROR, "fbr_delete failed to add NULL to dhtFileBlockQueueProcessor\n");
		}
		for(int i=0; i<(*fbr)->nfsFileBlockQueueProcessor->numThreads; i++) {
			int added = qp_add((*fbr)->nfsFileBlockQueueProcessor, NULL);
			if (!added) srfsLog(LOG_ERROR, "fbr_delete failed to add NULL to nfsFileBlockQueueProcessor\n");
		}
		qp_delete(&(*fbr)->dhtFileBlockQueueProcessor);
		qp_delete(&(*fbr)->nfsFileBlockQueueProcessor);
		rs_delete(&(*fbr)->rs);
		pg_delete(&(*fbr)->compressedPaths);
		pg_delete(&(*fbr)->noFBWPaths);
		pthread_spin_destroy(&(*fbr)->statLock);
		
		if ((*fbr)->pSession) {
			int	i;
			
			for (i = 0; i < FBR_DHT_THREADS; i++) {
				if ((*fbr)->pSession[i]) {
					delete (*fbr)->pSession[i];
					(*fbr)->pSession[i] = NULL;
				}
			}
		}
		
		mem_free((void **)fbr);
	} else {
		fatalError("bad ptr in fbr_delete");
	}
}

void fbr_parse_no_fbw_paths(FileBlockReader *fbr, char *paths) {
	pg_parse_paths(fbr->noFBWPaths, paths);
}

static int fbr_is_no_fbw_path(FileBlockReader *fbr, char *path) {
	srfsLog(LOG_FINE, "fbr_is_no_fbw_path %s", path);
	return pg_matches(fbr->noFBWPaths, path);
}

void fbr_parse_compressed_paths(FileBlockReader *fbr, char *paths) {
	pg_parse_paths(fbr->compressedPaths, paths);
}

static int fbr_is_compressed_path(FileBlockReader *fbr, char *path) {
	srfsLog(LOG_FINE, "fbr_is_compressed_path %s", path);
	return pg_matches(fbr->compressedPaths, path);
}

static void fbr_mark_op_failed(int numRequests, ActiveOpRef **refs, SRFSDHT* sd, char *file, int line) {
    sd_op_failed(sd, SKOperationState::FAILED, file, line);
    for (int i = 0; i < numRequests; i++) {
        ActiveOp    *op;
        
        op = refs[i]->ao;
		ao_set_stage(op, SRFS_OP_STAGE_DHT + 1);
		aor_delete(&refs[i]);
	}
}

static void fbr_process_dht_batch(void **requests, int numRequests, int curThreadIndex) {
	SKOperationState::SKOperationState	dhtMgetErr = SKOperationState::FAILED;
	FileBlockReader	*fbr;
	int				i;
    int             j;
	ActiveOpRef		*refs[numRequests];
	char			keys[numRequests][SRFS_FBID_KEY_SIZE];
	uint64_t		t1;
	uint64_t		t2;
    int             hasSKFSRequests;
    SKAsyncValueRetrieval   *pValRetrieval;
   	StrVector       requestGroup;  // sets of keys 
    int             isDuplicate[numRequests];

	srfsLog(LOG_FINE, "in fbr_process_dht_batch %d", curThreadIndex);
	fbr = NULL;
    pValRetrieval = NULL;

    memset(isDuplicate, 0, sizeof(int) * numRequests);
    
	// Create requestGroup
    hasSKFSRequests = FALSE;
	for (i = 0; i < numRequests; i++) {
		FileBlockReadRequest	*fbrr;
		ActiveOp	*op;

		refs[i] = (ActiveOpRef *)requests[i];
		op = refs[i]->ao;
		fbrr = (FileBlockReadRequest *)ao_get_target(op);
		fbrr_display(fbrr, LOG_FINE);
		if (fbr == NULL) {
			fbr = fbrr->fileBlockReader;
		} else {
			if (fbr != fbrr->fileBlockReader) {
				fatalError("multi fbr batch");
			}
		}
        // Simple heuristic to display errors when pure SKFS block reads fail
        // FUTURE - handle all cases
        if (!fid_is_native_fs(fbid_get_id(fbrr->fbid))) {
            hasSKFSRequests = TRUE;
        }
		fbid_to_string(fbrr->fbid, keys[i]);
		srfsLog(LOG_FINE, "fbr adding to group %llx %s", keys[i], keys[i]);
        requestGroup.push_back(keys[i]);
	}
	
	// Retrieve from kvs
	srfsLog(LOG_FINE, "fbr_process_dht_batch call get %d %d %d %d", numRequests, requestGroup.size(), defaultChecksum, defaultCompression);
    try {
    	t1 = curTimeMillis();
        pValRetrieval = fbr->ansp[curThreadIndex]->get(&requestGroup);
    	// FUTURE - could loop through completed results instead of blocking up front
        // (would pipeline the result processing)
        pValRetrieval->waitForCompletion();
	    t2 = curTimeMillis();
	    rts_add_sample(fbr->rtsDHT, t2 - t1, numRequests);
        dhtMgetErr = pValRetrieval->getState();
    } catch (SKRetrievalException & e ){
        // No need to handle duplicates here as each ppval is created on each iteration
		for (i = 0; i < numRequests; i++) {
			ActiveOp		      *op;
			FileBlockReadRequest  *fbrr;
			int				      successful;
			SKOperationState::SKOperationState  opState;

			op   = NULL;
			fbrr = NULL;
			successful  = FALSE;
			op = refs[i]->ao;
			fbrr = (FileBlockReadRequest *)ao_get_target(op);
			opState = e.getOperationState(keys[i]);
			if (opState == SKOperationState::SUCCEEDED) {
				SKStoredValue   *pStoredVal; 
                
				pStoredVal = NULL;
				pStoredVal = e.getStoredValue(keys[i]);
				if (pStoredVal == NULL) {
					//assume op has failed
					sd_op_failed(fbr->sd, dhtMgetErr, __FILE__, __LINE__);
					srfsLog(LOG_WARNING, "fbr dhtErr no pStoredVal %s %d line %d", keys[i], opState, __LINE__);
				} else {
					SKVal   *ppval;
                    
					ppval = pStoredVal->getValue();
					if (ppval == NULL ) {  
						//assume op has failed
						sd_op_failed(fbr->sd, dhtMgetErr, __FILE__, __LINE__);
						srfsLog(LOG_WARNING, "fbr dhtErr no pval %llx %s : %s line %d", fbrr, SKFS_FB_NS, keys[i], __LINE__);
					} else {
						if ((ppval->m_len == 0) || (ppval->m_len > SRFS_BLOCK_SIZE)) {
							srfsLog(LOG_WARNING, "Ignoring block with bogus size fbid->block %d m_len %d %s %d", fbrr->fbid->block, ppval->m_len, __FILE__, __LINE__);
						} else {
							CacheStoreResult	result;
								
                            // FUTURE - could add sanity check of this specific block size
                            srfsLog(LOG_FINE, "set op complete %llx %s %d", op, __FILE__, __LINE__);
                            ao_set_complete(op, AOResult_Success, ppval->m_pVal, ppval->m_len);
						    successful = TRUE;
                            srfsLog(LOG_FINE, "Storing block cache");
                            result = fbc_store_dht_value(fbrr->fileBlockReader->fileBlockCache, fbrr->fbid, ppval, fbrr->minModificationTimeMicros);
                            if (result != CACHE_STORE_SUCCESS) {
                                srfsLog(LOG_FINE, "Cache store rejected");
                                sk_destroy_val(&ppval);
                            }
						}
					}
					delete pStoredVal;
				}
			} //opState == SKOperationState::SUCCEEDED
			 else { //SKOperationState::INCOMPLETE or SKOperationState::FAILED 
				SKFailureCause::SKFailureCause  cause;
                
				cause = SKFailureCause::ERROR;
				if (opState == SKOperationState::FAILED){
					try {
						cause = e.getFailureCause(keys[i]); 
					} catch(SKClientException & e2) {
                        srfsLog(LOG_ERROR, "fbr getFailureCause exception at %s:%d\n%s\n", __FILE__, __LINE__, e2.what()); 
                    } catch(std::exception& e2) {
                        srfsLog(LOG_ERROR, "fbr getFailureCause exception at %s:%d\n%s\n", __FILE__, __LINE__, e2.what()); 
                    } catch (...) {
                        srfsLog(LOG_WARNING, "exception fbr failed to query FailureCause"); 
                    }
				}
				//mark all except SKFailureCause::NO_SUCH_VALUE
				if (cause == SKFailureCause::NO_SUCH_VALUE) {
					srfsLog(LOG_FINE, "fbr dhtErr %llx %d %d %s : %s line %d", fbrr, opState, cause, SKFS_FB_NS, keys[i], __LINE__);
				} else {
					srfsLog(LOG_WARNING, "fbr SKRetrievalException line %d\n%s\n", __LINE__, e.what());
					srfsLog(LOG_WARNING, " %s\n",  e.getDetailedFailureMessage().c_str());
					sd_op_failed(fbr->sd, opState, __FILE__, __LINE__);
					if (opState == SKOperationState::FAILED) {
						srfsLog(LOG_WARNING, "fbr dhtErr %llx %d %d %s : %s line %d", fbrr, opState, cause, SKFS_FB_NS, keys[i], __LINE__);
					} else {
						srfsLog(LOG_WARNING, "fbr dhtErr %llx %d  %s : %s line %d", fbrr, opState, SKFS_FB_NS, keys[i], __LINE__);
					}
				}
			}

			if (successful) {
                // No need to set op complete here. Done above
			} else {
				srfsLog(LOG_FINE, "set op stage dht+1 %llx", op);
				ao_set_stage(op, SRFS_OP_STAGE_DHT + 1);
			}
			aor_delete(&refs[i]);
		}
		if (pValRetrieval) {
			pValRetrieval->close();
			delete pValRetrieval;
		}
		srfsLog(LOG_FINE, "out fbr_process_dht_batch");
		return;
    } catch (SKClientException & e ){
        srfsLog(LOG_ERROR, "fbr dhtErr exception at %s:%d\n%s\n", __FILE__, __LINE__, e.what());
        e.printStackTrace();
        fbr_mark_op_failed(numRequests, refs, fbr->sd, __FILE__, __LINE__);
        return;
        //fatalError("fbr_process_dht_batch SKClientException ",  __FILE__, __LINE__, e.what());
    } catch (exception & e ){
        srfsLog(LOG_ERROR, "fbr dhtErr exception at %s:%d\n%s\n ", __FILE__, __LINE__, e.what());
        fbr_mark_op_failed(numRequests, refs, fbr->sd, __FILE__, __LINE__);
        fatalError("fbr_process_dht_batch exception ",  __FILE__, __LINE__);
    } catch ( ... ){
        fbr_mark_op_failed(numRequests, refs, fbr->sd, __FILE__, __LINE__);
        fatalError("unknown exception in fbr_process_dht_batch ", __FILE__, __LINE__ );
    }

    if (dhtMgetErr == SKOperationState::FAILED) {
        SKFailureCause::SKFailureCause  errCause;
        
        try {
            if (pValRetrieval != NULL) {
                errCause = pValRetrieval->getFailureCause();
			    if (hasSKFSRequests) {
				    srfsLog(LOG_ERROR, "fbr failure cause %d %d ", dhtMgetErr, errCause);
				}
            }
        } catch(SKClientException & e) {
            srfsLog(LOG_ERROR, "fbr FailureCause at %s:%d\n%s\n", __FILE__, __LINE__, e.what()); 
        } catch (std::exception &e) {
            srfsLog(LOG_ERROR, "fbr getFailureCause exception at %s:%d\n%s\n", __FILE__, __LINE__, e.what());
        } catch (...) {
            srfsLog(LOG_ERROR, "exception fbr failed query cause"); 
        }
    } else {
        srfsLog(LOG_FINE, "fbr dht batch got %d", dhtMgetErr);
    }

    StrValMap   *pValues = pValRetrieval->getValues();
	OpStateMap  *opStateMap = pValRetrieval->getOperationStateMap();
    int foundDuplicate = FALSE;

    // Check for duplicates
    if (pValues->size() != numRequests) {
        // Naive n^2 search for the duplicates that must exist
        for (i = 0; i < numRequests; i++) {
            for (j = i + 1; j < numRequests; j++) {
                if (!strcmp(keys[i], keys[j])) {
                    isDuplicate[j] = TRUE;
                    foundDuplicate = TRUE;
                }
            }
        }
    }
    
    for (i = 0; i < numRequests; i++) {
        if (!isDuplicate[i]) {
            ActiveOp		      *op;
            FileBlockReadRequest  *fbrr;
            SKVal	              *val;
            int				successful;
            SKOperationState::SKOperationState  opState;

            op   = NULL;
            fbrr = NULL;
            val  = NULL;
            successful = FALSE;
            op = refs[i]->ao;
            fbrr = (FileBlockReadRequest *)ao_get_target(op);
                
            try {
                opState = opStateMap->at(keys[i]);
            } catch(std::exception &emap) { 
                opState = SKOperationState::FAILED;
                srfsLog(LOG_INFO, "fbr std::map exception at %s:%d\n%s\n", __FILE__, __LINE__, emap.what()); 
            }

            if (opState == SKOperationState::SUCCEEDED) {
                SKVal   *ppval;
            
                try {
                    ppval = pValues->at(keys[i]);
                } catch (std::exception &emap) { 
                    ppval = NULL;
                    srfsLog(LOG_INFO, "fbr std::map exception at %s:%d\n%s\n", __FILE__, __LINE__, emap.what()); 
                }
                if (ppval == NULL ) {  
                    //sd_op_failed(fbr->sd, opState, __FILE__, __LINE__);
                    //srfsLog(LOG_WARNING, "fbr dhtErr val not found %llx %s : %s line %d", fbrr, SKFS_FB_NS, keys[i], __LINE__);
                    // used to treat this as an error, but with new OpResult fix, we reach here for blocks that aren't found
                    srfsLog(LOG_FINE, "set op stage dht+1 %llx", op);
                    ao_set_stage(op, SRFS_OP_STAGE_DHT + 1);
                } else {
                    if (ppval->m_len > SRFS_BLOCK_SIZE) {
                        int ki;
                    //if ((ppval->m_len == 0) || (ppval->m_len > SRFS_BLOCK_SIZE)) {
                        //sd_op_failed(fbr->sd, opState);
                        srfsLog(LOG_WARNING, "Ignoring block with bogus size keys[i] %s fbid->block %d m_len %d dup %d %s %d", 
                            keys[i], fbrr->fbid->block, ppval->m_len, foundDuplicate, __FILE__, __LINE__);
                        for (ki = 0; ki < numRequests; ki++) {
                            srfsLog(LOG_WARNING, "keys[%d] %s", ki, keys[ki]);
                        }
                    } else {
                        CacheStoreResult	result;
                        
                        if (ppval->m_len == 0) {
                            if (ppval->m_pVal != NULL) {
                                srfsLog(LOG_WARNING, "Ignoring bogus empty value %s %d", __FILE__, __LINE__);
                            } else {
                                // FIXME - temp potential crash workaround
                                //sk_destroy_val(&ppval);
                            }
                            ppval = sk_create_val();
                            // FUTURE - Consider changing the below to remove a copy
                            // Would require a special check in cache data deletion to ensure that we don't delete
                            // the shared data. Maybe add support for no delete entries.
                            // Below will create a copy of the zero block for now.
                            sk_set_val(ppval, SRFS_BLOCK_SIZE, (void *)zeroBlock);
                        }
                        srfsLog(LOG_FINE, "set op complete %llx %s %d", op, __FILE__, __LINE__);
                        ao_set_complete(op, AOResult_Success, ppval->m_pVal, ppval->m_len);
                        successful = TRUE;
                        srfsLog(LOG_FINE, "Storing block cache");
                        result = fbc_store_dht_value(fbrr->fileBlockReader->fileBlockCache, fbrr->fbid,
                                                     ppval, fbrr->minModificationTimeMicros);
                        if (result != CACHE_STORE_SUCCESS) {
                            srfsLog(LOG_FINE, "Cache store rejected");
                            sk_destroy_val(&ppval);
                        }
                    }
                }
            } //opState == SKOperationState::SUCCEEDED
             else { //SKOperationState::INCOMPLETE or SKOperationState::FAILED 
                SKFailureCause::SKFailureCause cause = SKFailureCause::ERROR;
                
                if (opState == SKOperationState::FAILED){
                    try {
                        cause = pValRetrieval->getFailureCause();
                    } catch(SKClientException &e) { 
                        srfsLog(LOG_WARNING, "fbr getFailureCause exception at %s:%d\n%s\n", __FILE__, __LINE__, e.what()); 
                    } catch(std::exception &e) { 
                        srfsLog(LOG_WARNING, "fbr getFailureCause exception at %s:%d\n%s\n", __FILE__, __LINE__, e.what()); 
                    } catch (...) {
                        srfsLog(LOG_WARNING, "exception fbr failed to query FailureCause"); 
                    }
                }
                //mark all except SKFailureCause::NO_SUCH_VALUE
                if (cause != SKFailureCause::NO_SUCH_VALUE) {
                    sd_op_failed(fbr->sd, dhtMgetErr, __FILE__, __LINE__);
                }
                            
                if (!fid_is_native_fs(fbid_get_id(fbrr->fbid))) {
                    fbc_remove_active_op(fbrr->fileBlockReader->fileBlockCache, fbrr->fbid);
                    if (cause == SKFailureCause::NO_SUCH_VALUE) {
                        ao_set_complete_error(op, ENOENT);                    
                    } else {
                        ao_set_complete_error(op, EIO);
                    }
                    successful = TRUE; // FUTURE - change the name
                                      // this is a notification that we don't need to update the operation
                }            
                
                srfsLog(LOG_FINE, "fbr m_rc %llx %d %d %s : %s  %d", fbrr, opState, cause, SKFS_FB_NS, keys[i], __LINE__);
            }

            if (successful) {
                // No need to set op complete. Taken care of above
            } else {
                srfsLog(LOG_FINE, "set op stage dht+1 %llx", op);
                ao_set_stage(op, SRFS_OP_STAGE_DHT + 1);
            }
        } else {
            // No duplicate-specific action required
        }
        aor_delete(&refs[i]);
    }
    delete opStateMap;
    delete pValues;
    pValRetrieval->close();
    delete pValRetrieval;

	srfsLog(LOG_FINE, "out fbrr_process_dht_batch");
}


static void fbr_sanity_check_read(FileBlockReadRequest *fbrr, size_t blockSize) {
	if ((blockSize > SRFS_BLOCK_SIZE) || (!fbid_is_last_block(fbrr->fbid) && blockSize != SRFS_BLOCK_SIZE)) {
		char	key[SRFS_FBID_KEY_SIZE];

		fbid_to_string(fbrr->fbid, key);
        if (fbid_is_last_block(fbrr->fbid) && blockSize == SRFS_BLOCK_SIZE) {
            srfsLog(LOG_ERROR, "suspicious block size %lu", blockSize);
            fatalError("suspicious block size", __FILE__, __LINE__);
        }
		srfsLog(LOG_ERROR, "Bogus block read attempt %lu %s", blockSize, key);
		fatalError("Bogus block read attempt", __FILE__, __LINE__);
	} else {
		if (srfsLogLevelMet(LOG_FINE)) {
			char	key[SRFS_FBID_KEY_SIZE];

			fbid_to_string(fbrr->fbid, key);
			srfsLog(LOG_FINE, "Clean block read attempt %lu %s", blockSize, key);
		}
	}
}

static int fbr_get_block_file(char *blockFile, char *path, FileBlockReadRequest *fbrr) {
	int		index2;
	int		index1;
	char	*char2;
	char	*char1;

	char2 = strrchr(path, '/');
	index2 = (int)(char2 - path);
	if (index2 >= 0) {
		char1 = path_prev(char2);
		index1 = (int)(char1 - path);
		if (index1 < 0) {
			fatalError("bad index1", __FILE__, __LINE__);
		}
		strncpy(blockFile, path, index1 + 1);
		blockFile[index1 + 1] = '.';
		strncpy(blockFile + index1 + 2, char1 + 1, index2 - index1 - 1);
		sprintf(blockFile + index2 + 1, "/.%s/%lu", path + index2 + 1, fbid_get_block(fbrr->fbid));
		srfsLog(LOG_FINE, "blockFile %s", blockFile);
		return 0;
	} else {
		return -1;
	}
}

static void *fbr_read_block_raw(FileBlockReadRequest *fbrr, size_t *_blockSize, char *path, int *openOK) {
	NativeFileReference *nfr;
    int fd;

    nfr = nft_open(fbrr->fileBlockReader->nft, path);
    fd = nfr_get_fd(nfr);
	//fd = open(path, O_RDONLY | O_NOFOLLOW);
	*openOK = fd != -1;
	if (fd != -1) {
		void	*blockData;
		unsigned char *dest;
		ssize_t	totalRead;
		ssize_t	numRead;
		ssize_t	blockSize;
		off_t	offset;
		int		result;
		int		zeroReadRetries;
    
		offset  = fbid_block_offset(fbrr->fbid);
		blockSize = fbid_block_size(fbrr->fbid);
		//srfsLog(LOG_FINE, "offset %ld blockSize %ld", offset, blockSize);
		blockData = mem_alloc(1, blockSize);
		dest = (unsigned char *)blockData;
		totalRead = 0;
		zeroReadRetries = 0;
		while (totalRead < blockSize) {
			numRead = pread(fd, dest, blockSize - totalRead, offset);
			if (numRead > 0) {
				totalRead += numRead;
				dest += numRead;
				zeroReadRetries = 0;
			} else {
				char	key[SRFS_FBID_KEY_SIZE];

				srfsLog(LOG_WARNING, "Error reading block %s", path);
				fbid_to_string(fbrr->fbid, key);
				srfsLog(LOG_WARNING, "fbid %s", key);
				srfsLog(LOG_WARNING, "numRead %lu totalRead %lu dest %llx blockData %llx blockSize %lu totalRead %lu blockSize - totalRead %lu offset %lu", 
										numRead, totalRead, dest, blockData, blockSize, totalRead, blockSize - totalRead, offset);
                if (nfr != NULL) {
                    nfr_delete(&nfr);
                } else {
                    close(fd);
                }
				if (numRead < 0 || zeroReadRetries > maxZeroReadRetries) {
					srfsLog(LOG_ERROR, "Giving up on read");
					mem_free(&blockData);
					return NULL;
				} else {
					srfsLog(LOG_ERROR, "Retrying read");
					sleep(1 << zeroReadRetries);
					zeroReadRetries++;
					fd = open(path, O_RDONLY | O_NOFOLLOW);
					if (fd == -1) {
						srfsLog(LOG_WARNING, "open failed");
						close(fd);
						mem_free(&blockData);
						return NULL;
					}
				}
			}
		}
        if (nfr != NULL) {
            result = nfr_delete(&nfr);
        } else {
            result = close(fd);
        }
		if (result == -1) {
			srfsLog(LOG_WARNING, "Ignoring error closing %s", path);
		}
		*_blockSize = (size_t)blockSize;
		fbr_sanity_check_read(fbrr, blockSize);
		return blockData;
	} else {
		return NULL;
	}
}

// read a block from the native file system
static void *fbr_read_block(FileBlockReadRequest *fbrr, size_t *_blockSize, int *cacheInDHT) {
	FileID	*fid;
	PathListEntry	*pathListEntry;
	char	*path;

	*_blockSize = 0;
	fid = fbid_get_id(fbrr->fbid);
	pathListEntry = f2p_get(fbrr->fileBlockReader->f2p, fid);
	if (pathListEntry == NULL) {
		srfsLog(LOG_WARNING, "Can't find path for fid");
		return NULL;
	} else {
		while (pathListEntry != NULL) {
			int		openOK;
			void	*blockData;

			path = pathListEntry->path;
			*cacheInDHT = !fbr_is_no_fbw_path(fbrr->fileBlockReader, path);
			if (path == NULL) {
				fatalError("Unexpected NULL path", __FILE__, __LINE__);
			}

            srfsLog(LOG_FINE, "calling fbr_read_block_raw %s", path);
			blockData = fbr_read_block_raw(fbrr, _blockSize, path, &openOK);
			if (openOK) {
				pthread_spin_lock(&fbrr->fileBlockReader->statLock);
				fbrr->fileBlockReader->directNFS++;
				pthread_spin_unlock(&fbrr->fileBlockReader->statLock);
				return blockData;
			} else {
				struct stat	stbuf;
				int	statResult;

				statResult = lstat(path, &stbuf);
				if (statResult == 0) {
					if (S_ISLNK(stbuf.st_mode)) {
						char	buf[stbuf.st_size];
						int		readlinkResult;

						readlinkResult = readlink(path, buf, stbuf.st_size);
						srfsLog(LOG_FINE, "readLink %s %d %d", path, stbuf.st_size, readlinkResult);
						if (readlinkResult != -1) {
							void	*blockData;

							blockData = mem_alloc(1, readlinkResult + 1);
							memcpy(blockData, buf, readlinkResult);
							*_blockSize = readlinkResult;
							return blockData;
						} else {
							srfsLog(LOG_WARNING, "FileBlockReader unable to open %s\t%s%d", path, __FILE__, __LINE__);
							return NULL;
						}
					} else {
						srfsLog(LOG_WARNING, "FileBlockReader unable to open %s\t%s%d", path, __FILE__, __LINE__);
						return NULL;
					} 
				} else {
					srfsLog(LOG_WARNING, "FileBlockReader unable to open %s\t%s%d. Trying next PathListEntry", path, __FILE__, __LINE__);
					pathListEntry = pathListEntry->next;
				}
			}
		}
		srfsLog(LOG_WARNING, "No more path list entries");
		return NULL;
	}
}

static void fbr_process_nfs_request(void *_requestOpRef, int curThreadIndex) {
	FileBlockReadRequest	*fbrr;
	ActiveOpRef	*aor;
	ActiveOp	*op;
	void		*blockData;
	size_t		blockSize;
	uint64_t	t1;
	uint64_t	t2;
	int			cacheInDHT;

	srfsLog(LOG_FINE, "in fbr_process_nfs_request %d %llx", curThreadIndex, _requestOpRef);
	aor = (ActiveOpRef *)_requestOpRef;
	op = aor->ao;
	fbrr = (FileBlockReadRequest *)ao_get_target(op);
	fbrr_display(fbrr, LOG_FINE);

	cacheInDHT = TRUE;
	t1 = curTimeMillis();
	blockData = fbr_read_block(fbrr, &blockSize, &cacheInDHT);
	t2 = curTimeMillis();
	rts_add_sample(fbrr->fileBlockReader->rtsNFS, t2 - t1, 1);
	if (blockData != NULL) {
		CacheStoreResult	result;
        void		        *blockDataForWrite;
		
        srfsLog(LOG_FINE, "set op complete %llx %s %d", op, __FILE__, __LINE__);
        ao_set_complete(op, AOResult_Success, blockData, blockSize);
        blockDataForWrite = mem_dup(blockData, blockSize);
		srfsLog(LOG_FINE, "Storing block cache %d", blockSize);
		result = fbc_store_raw_data(fbrr->fileBlockReader->fileBlockCache, fbrr->fbid,      
                            blockData, blockSize, TRUE, fbrr->minModificationTimeMicros);
		if (result == CACHE_STORE_SUCCESS) {
			if (cacheInDHT) {
                fbw_write_file_block(fbrr->fileBlockReader->fbwCompress, fbrr->fbid, blockSize, blockDataForWrite, NULL);
                //fbw_write_file_block(fbrr->fileBlockReader->fbwCompress, fbrr->fbid, blockSize, blockData, aor_new(op, __FILE__, __LINE__));
			} else {
				srfsLog(LOG_FINE, "non fbw path");
			}
		} else {
			srfsLog(LOG_FINE, "Cache store rejected");
			mem_free(&blockData);
			mem_free(&blockDataForWrite);
		}
	} else {
        srfsLog(LOG_FINE, "set op complete error %llx %d %s %d", op, ETIMEDOUT, __FILE__, __LINE__);
        fbc_remove_active_op(fbrr->fileBlockReader->fileBlockCache, fbrr->fbid);
        ao_set_complete_error(op, ETIMEDOUT);                    
		// Current error approach: send back empty cache entry to the sender
		// Sender will take appropriate action based on that.
		//srfsLog(LOG_FINE, "Storing error cache");
		////fbc_store_raw_data(fbrr->fileBlockReader->fileBlockCache, fbrr->path, fbrr->dest);
        //fbc_store_error(fbrr->fileBlockReader->fileBlockCache, fbrr->fbid, -1,
        //                fbrr->minModificationTimeMicros, _FBR_ERR_TIMEOUT_MILLIS);
	}
    // No need to set op complete here. Taken care of above
	aor_delete(&aor);
	srfsLog(LOG_FINE, "out fbr_process_nfs_request %llx", _requestOpRef);
}

ActiveOp *fbr_create_active_op(void *_fbr, void *_fbid, uint64_t minModificationTimeMicros) {
	FileBlockReader	*fbr;
	FileBlockID		*fbid;
	ActiveOp		*op;
	FileBlockReadRequest	*fileBlockReadRequest;

	fbr = (FileBlockReader *)_fbr;
	fbid = (FileBlockID *)_fbid;
	srfsLog(LOG_FINE, "fb_create_active_op %llx", fbid);
	fileBlockReadRequest = fbrr_new(fbr, fbid, minModificationTimeMicros);
	fbrr_display(fileBlockReadRequest, LOG_FINE);
	op = ao_new(fileBlockReadRequest, (void (*)(void **))fbrr_delete);
	return op;
}

static void fbr_cp_rVal_to_pbrr(PartialBlockReadRequest *pbrr, ActiveOpRef *aor, char *file, int line) {
    size_t  rValLength;
    
    rValLength = aor_get_rValLength(aor);
    if (pbrr->readOffset + pbrr->readSize > rValLength) {
        fatalError("pbrr->readOffset + pbrr->readSize > rValLength", file, line); 
    }
    memcpy(pbrr->dest, aor_get_rVal(aor) + pbrr->readOffset, pbrr->readSize);
}

// Main FileBlockReader function. Handles a group of partial block read requests.
int fbr_read(FileBlockReader *fbr, PartialBlockReadRequest **pbrrs, int numRequests,
								   PartialBlockReadRequest **pbrrsReadAhead, int numRequestsReadAhead,
                                   int presumeBlocksInDHT,
                                   int useNFSReadAhead) {
    AOResult        aoResults[numRequests];
	CacheReadResult	results[numRequests];
	ActiveOpRef		*activeOpRefs[numRequests];
	int				cacheNumRead[numRequests];
	char			statCounted[numRequests];
	int				returnCode;
	int				i;
	uint64_t		timeout;

	returnCode = 0;
	srfsLog(LOG_FINE, "in fbr_read");
	for (i = 0; i < numRequests; i++) {
		activeOpRefs[i] = NULL;
		cacheNumRead[i] = 0;
        aoResults[i] = AOResult_Incomplete;
	}
	
	// This function works on the group of requests for efficiency.

	// First look in the cache, and begin key value store requests for any missing values
	srfsLog(LOG_FINE, "looking in cache for block");
	for (i = 0; i < numRequests; i++) {
		results[i] = fbc_read(fbr->fileBlockCache, pbrrs[i]->fbid, (unsigned char *)pbrrs[i]->dest, 
							pbrrs[i]->readOffset, pbrrs[i]->readSize, &activeOpRefs[i], &cacheNumRead[i], fbr, pbrrs[i]->minModificationTimeMicros, _FBR_READ_OP_TIMEOUT_MILLIS);
		statCounted[i] = 0;
		srfsLog(LOG_FINE, "cache results[%d] %d cacheNumRead[i] %d", i, results[i], cacheNumRead[i]);
		if (results[i] == CRR_ACTIVE_OP_CREATED) {
			ActiveOp 	*op;
			ActiveOpRef	*aor;
			int			added;

			op = activeOpRefs[i]->ao;
			srfsLog(LOG_FINE, "CRR_ACTIVE_OP_CREATED %llx", pbrrs[i]->fbid);
			srfsLog(LOG_FINE, "Queueing op %llx %llx", pbrrs[i]->fbid, op);
			aor = aor_new(op, __FILE__, __LINE__);
            // value was not in the cache, look in the kvs
			added = qp_add(fbr->dhtFileBlockQueueProcessor, aor);
			if (!added) {
				aor_delete(&aor);
                // FUTURE - currently, any rejected requests are still waited on (below)
                // as if the kvs request is active. Could remove that wait
			}
		} else if (results[i] == CRR_FOUND) {
			if ((unsigned int)cacheNumRead[i] == pbrrs[i]->readSize) {
				rs_cache_inc(fbr->rs);
				statCounted[i] = TRUE;
                aoResults[i] = AOResult_Success;
			} else {
                srfsLog(LOG_WARNING, "1) cacheNumRead[%d] (%d) != pbrrs[%d]->readSize (%d)", 
                                     i, cacheNumRead[i], i, pbrrs[i]->readSize);
                fatalError("Error: cacheNumRead[i] != pbrrs[i]->readSize", __FILE__, __LINE__);
			}
		}
	}

	// handle speculative read ahead requests
	srfsLog(LOG_FINE, "read ahead");
	for (i = 0; i < numRequestsReadAhead; i++) {
		CacheReadResult	cacheReadResult;
		ActiveOpRef		*aor;

		aor = NULL;
		cacheReadResult = fbc_read(fbr->fileBlockCache, pbrrsReadAhead[i]->fbid, NULL, 
									0, 0, &aor, NULL, fbr, 
                                    pbrrsReadAhead[i]->minModificationTimeMicros,
                                    _FBR_PREFETCH_OP_TIMEOUT_MILLIS);
		if (cacheReadResult == CRR_ACTIVE_OP_CREATED) {
			int	added;
            //char	fbid[SRFS_FBID_KEY_SIZE];

            //fbid_to_string(pbrrsReadAhead[i]->fbid, fbid);

			// Not found in cache. Issue the read ahead, but don't hold on to the ref
			// since we don't need the result.
			//srfsLog(LOG_WARNING, "Ahead: CRR_ACTIVE_OP_CREATED %s", fbid);
			//srfsLog(LOG_WARNING, "Ahead: Queueing op %llx %llx", pbrrsReadAhead[i]->fbid, aor->ao);
            if (!useNFSReadAhead) {
                added = qp_add(fbr->dhtFileBlockQueueProcessor, aor);
            } else {
                srfsLog(LOG_FINE, "adding request %d to nfs q", i);
                added = qp_add(fbr->nfsFileBlockQueueProcessor, aor);
            }
			if (!added) {
				aor_delete(&aor);
			} else {
                // FUTURE - consider making this optional
                // to date haven't found a benefit
                /*
                    ActiveOpRef *aor2;
                    int	added2;

                    aor2 = NULL;
                    srfsLog(LOG_FINE, "adding request %d to nfs q", i);
                    aor2 = aor_new(aor->ao, __FILE__, __LINE__);
                    added2 = qp_add(fbr->nfsFileBlockQueueProcessor, aor2);
                    if (!added2) {
                        aor_delete(&aor2);
                    }
                */
            }
		} else {
            if (aor != NULL) {
				aor_delete(&aor);
            }
        }
	}

    // Now wait for kvs request completion, and begin native fs requests for any missing values.
    if (presumeBlocksInDHT) {
        timeout = sd_get_dht_timeout(fbr->sd, fbr->rtsDHT, fbr->rtsNFS, numRequests);
    } else {
        timeout = 0;
    }
	srfsLog(LOG_FINE, "stage timeout %u", timeout);
	for (i = 0; i < numRequests; i++) {
        if (aoResults[i] != AOResult_Success) {
			aoResults[i] = aor_wait_for_stage_timed(activeOpRefs[i], SRFS_OP_STAGE_DHT, timeout);
			if (aoResults[i] != AOResult_Success) {
				if (fid_is_native_fs(&pbrrs[i]->fbid->fid)) {
                    int         added;
                    ActiveOpRef *aor;

                    aor = NULL;
					srfsLog(LOG_FINE, "adding request %d to nfs q", i);
                    aor = aor_new(activeOpRefs[i]->ao, __FILE__, __LINE__);
					added = qp_add(fbr->nfsFileBlockQueueProcessor, aor);
                    if (!added) {
                        // nfsFileBlockQueueProcessor blocks if full, so addition will not fail
                        fatalError("panic", __FILE__, __LINE__);
                    }
				}
			} else {
                fbr_cp_rVal_to_pbrr(pbrrs[i], activeOpRefs[i], __FILE__, __LINE__);
                cacheNumRead[i] = pbrrs[i]->readSize;
				if (!statCounted[i]) {
					statCounted[i] = TRUE;
					rs_dht_inc(fbr->rs);
				}
			}
		}
	}

	// FUTURE - stats don't accurately track nfs/dht since both can be trying

	// Wait for incomplete results
	for (i = 0; i < numRequests; i++) {
		srfsLog(LOG_FINE, "results[%d] %d", i, results[i]);
        if (aoResults[i] != AOResult_Success && aoResults[i] != AOResult_Error) {
			if (fid_is_native_fs(&pbrrs[i]->fbid->fid)) {
                aoResults[i] = aor_wait_for_stage_timed(activeOpRefs[i], AO_STAGE_COMPLETE, FBR_NFS_STAGE_TIMEOUT_MS);
            } else {
                aoResults[i] = aor_wait_for_stage_timed(activeOpRefs[i], SRFS_OP_STAGE_DHT,
                                         FBR_DHT_STAGE_WRITABLE_FS_TIMEOUT_MS);
            }
			if (!statCounted[i]) {
				statCounted[i] = TRUE;
				if (fid_is_native_fs(&pbrrs[i]->fbid->fid)) {
					rs_nfs_inc(fbr->rs);
				} else {
					rs_dht_inc(fbr->rs);
				}
			}
            if (aoResults[i] != AOResult_Success) {            
                char	fbid[SRFS_FBID_KEY_SIZE];

                fbid_to_string(pbrrs[i]->fbid, fbid);
				srfsLog(LOG_ERROR, "%d %s %llx %d %d %llx %llx", pbrrs[i]->fbid, fbid, pbrrs[i]->dest, 
					pbrrs[i]->readOffset, pbrrs[i]->readSize, &activeOpRefs[i], &cacheNumRead[i]);
				srfsLog(LOG_ERROR, "results[i] %s", crr_strings[results[i]]);
				srfsLog(LOG_ERROR, "activeOpRefs[i]->ao %llx", activeOpRefs[i]->ao);
				srfsLog(LOG_ERROR, "fbr_read failed");
				returnCode = -1;
                if (!fid_is_native_fs(&pbrrs[i]->fbid->fid)) {
                    fbc_remove_active_op(fbr->fileBlockCache, pbrrs[i]->fbid);
                }
			} else {
                fbr_cp_rVal_to_pbrr(pbrrs[i], activeOpRefs[i], __FILE__, __LINE__);
                cacheNumRead[i] = pbrrs[i]->readSize;
            }
		} else {
            if (aoResults[i] == AOResult_Success) {
                if (activeOpRefs[i] != NULL) {
                    fbr_cp_rVal_to_pbrr(pbrrs[i], activeOpRefs[i], __FILE__, __LINE__);
                    cacheNumRead[i] = pbrrs[i]->readSize;
                    if (!statCounted[i]) {
                        statCounted[i] = TRUE;
                        rs_dht_inc(fbr->rs);
                    }
                }
            } else if (aoResults[i] == AOResult_Error) {
                returnCode = -1;
			} else {
                fatalError("panic", __FILE__, __LINE__);
            }
		}
		if (activeOpRefs[i] != NULL) {
			srfsLog(LOG_FINE, "Deleting ref %llx", &activeOpRefs[i]);
			aor_delete(&activeOpRefs[i]);
		}
	}
	if (returnCode == 0) {
		for (i = 0; i < numRequests; i++) {
			returnCode += cacheNumRead[i];
		}
	} else {
		for (i = 0; i < numRequests; i++) {
            if (activeOpRefs[i] != NULL) {
                srfsLog(LOG_FINE, "Deleting ref %llx", &activeOpRefs[i]);
                aor_delete(&activeOpRefs[i]);
            }
        }
    }
	// nfs read-ahead disabled for now
	// consider adding back in eventually
	// nfs read-ahead should be disabled when nfs delays are > a threshold
#if 0
        // handle speculative read ahead requests
        srfsLog(LOG_FINE, "nfs read ahead %d", numRequestsReadAhead);
        for (i = 0; i < numRequestsReadAhead; i++) {
            CacheReadResult	cacheReadResult;
            ActiveOpRef		*aor;

            aor = NULL;
            cacheReadResult = fbc_read(fbr->fileBlockCache, pbrrsReadAhead[i]->fbid, NULL, 
                                        0, 0, &aor, NULL, fbr, 
                                        pbrrsReadAhead[i]->minModificationTimeMicros,
                                        _FBR_PREFETCH_OP_TIMEOUT_MILLIS);
            srfsLog(LOG_FINE, "nfs read ahead crr %d", (int)cacheReadResult);
            if (cacheReadResult == CRR_ACTIVE_OP_CREATED) {
                int	added;

                // not found in cache, issue the read ahead, but don't hold on to the ref
                // since we don't need the result
                srfsLog(LOG_FINE, "Ahead: CRR_ACTIVE_OP_CREATED %llx", pbrrsReadAhead[i]->fbid);
                srfsLog(LOG_FINE, "Ahead: Queueing op %llx %llx", pbrrsReadAhead[i]->fbid, aor->ao);
                added = qp_add(fbr->nfsFileBlockQueueProcessor, aor);
                if (!added) {
                    aor_delete(&aor);
                }
            }
        }
#endif
	srfsLog(LOG_FINE, "out fbr_read");
	return returnCode;
}

int fbr_read_test(FileBlockReader *fbr, FileBlockID *fbid, void *dest, size_t readOffset, size_t readSize) {
	PartialBlockReadRequest	**pbrrs;
	PartialBlockReadRequest	*pbrr;
	int	result;

	pbrr = pbrr_new(fbid, dest, readOffset, readSize, 0);
	pbrrs = &pbrr;
	result = fbr_read(fbr, pbrrs, 1, NULL, 0, TRUE, FALSE);
	pbrr_delete(&pbrr);
	pbrrs = NULL;
	return result;
}

void fbr_parse_permanent_suffixes(FileBlockReader *fbReader, char *permanentSuffixes) {
	fbc_parse_permanent_suffixes(fbReader->fileBlockCache, permanentSuffixes);
}

void fbr_display_stats(FileBlockReader *fbr, int detailedStats) {
	srfsLog(LOG_WARNING, "FileBlockReader Stats");
	rs_display(fbr->rs);
	if (detailedStats) {
		fbc_display_stats(fbr->fileBlockCache);
	}
    srfsLog(LOG_WARNING, "fbr ResponseTimeStats: DHT");
    rts_display(fbr->rtsDHT);
    srfsLog(LOG_WARNING, "fbr ResponseTimeStats: NFS");
    rts_display(fbr->rtsNFS);
	pthread_spin_lock(&fbr->statLock);
    srfsLog(LOG_WARNING, "NFS source");
	srfsLog(LOG_WARNING, "directNFS: \t%lu", fbr->directNFS);
	srfsLog(LOG_WARNING, "compressedNFS: \t%lu", fbr->compressedNFS);
	pthread_spin_unlock(&fbr->statLock);
}
