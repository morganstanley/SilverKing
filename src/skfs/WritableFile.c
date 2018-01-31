// WritableFile.c

/////////////
// includes

#include <errno.h>
#include <fuse.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "AttrReader.h"
#include "FileBlockWriter.h"
#include "SKFSOpenFile.h"
#include "WritableFile.h"
#include "WritableFileBlock.h"
#include "Util.h"


////////////////////
// private defines

#define WF_MAGIC	0xabac
#define WF_INITIAL_BLOCK_SIZE	8
#define WF_MAX_BLOCK_SIZE		1024
#define WF_BLOCK_INCREMENT		16
#define WF_MAX_BLOCKS_OUTSTANDING	100
#define WF_ATTR_WRITE_MAX_ATTEMPTS	12
#define WF_TRUNCATE_ON_FLUSH_ERROR  1
#define WF_FAILED_BLOCK_RETRIES 2

// Ugly Linux requirement to report st_blocks as 512 byte blocks
// irrespective of actual blocks
//#define statBlockConversion(B) (B * (SRFS_BLOCK_SIZE / 512))
// For now, roll back this change. Need to remove assumption
// in code that actual blocks == st_blocks if we use this fix...
// Perhaps just do the fix in stat...
#define statBlockConversion(B) (B)

/////////////////
// private types

typedef struct WF_BlockWrite {
	WritableFileBlock	*wfb;
	FBW_ActiveDirectPut	*adp; // freed by FileBlockWriter
	SKOperationState::SKOperationState	result;
} WF_BlockWrite;


/////////////////
// private data

static int wf_syncDirUpdates;

///////////////////////
// private prototypes

static int wf_init_cur_block(WritableFile *wf, PartialBlockReader *pbr);
static void wf_init_blockList(WritableFile *wf, size_t sizes);
static void wf_write_block(WritableFile *wf, WritableFileBlock *wfb, uint64_t block, FileBlockWriter *fbw);
static SKOperationState::SKOperationState wf_write_block_sync(WritableFile *wf, WritableFileBlock *wfb, uint64_t blockIndex, FileBlockWriter *fbw, int maxAttempts = 1);
static void wf_new_block(WritableFile *wf);
static uint64_t wf_blocks_outstanding(WritableFile *wf);
static void wf_limit_outstanding_blocks(WritableFile *wf, FileBlockWriter *fbw, uint64_t limit);
static int wf_all_blocks_written_successfully(WritableFile *wf, FileBlockWriter *fbw);
static int wf_blocks_written_successfully(WritableFile *wf, size_t numBlocks, FileBlockWriter *fbw);
static uint64_t wf_cur_block_index(WritableFile *wf);
static int wf_update_attr(WritableFile *wf, AttrWriter *aw, AttrCache *ac, int cacheOnly = FALSE);
static int _wf_find_empty_ref(WritableFile *wf);
static int _wf_has_references(WritableFile *wf);
static int wf_close(WritableFile *wf, AttrWriter *aw, FileBlockWriter *fbw, AttrCache *ac);
static int _wf_flush(WritableFile *wf, AttrWriter *aw, FileBlockWriter *fbw, AttrCache *ac);
static off_t wf_bytes_successfully_written(WritableFile *wf, FileBlockWriter *fbw);

///////////////////
// implementation

void wf_set_sync_dir_updates(int syncDirUpdates) {
    srfsLog(LOG_INFO, "wf_set_sync_dir_updates %d", syncDirUpdates);
    wf_syncDirUpdates = syncDirUpdates;
}

static WF_BlockWrite *wfbw_new(WritableFileBlock *wfb, FBW_ActiveDirectPut *adp) {
	WF_BlockWrite	*bw;

	bw = (WF_BlockWrite *)mem_alloc(1, sizeof(WF_BlockWrite));
	if (srfsLogLevelMet(LOG_FINE)) {
		srfsLog(LOG_FINE, "wfbw_new:\t%llx\t%llx\n", wfb, adp);
	}
	bw->wfb = wfb;
	bw->adp = adp;
	bw->result = SKOperationState::INCOMPLETE;
	return bw;
}

static void wfbw_clear(WF_BlockWrite *bw) {
    if (bw->wfb != NULL) {
		fatalError("bw->wfb != NULL");
    }
    if (bw->adp != NULL) {
		fatalError("bw->adp != NULL");
    }
    memset(bw, 0, sizeof(WF_BlockWrite));
}

/*
deletion is performed by the ArrayBlockList
wfb is freed by wfbw_set_complete
adp is freed by the FileBlockWriter
static void wfbw_delete(WF_BlockWrite **bw) {
	if (bw != NULL && *bw != NULL) {
		if ((*bw)->wfb != NULL) {
			wfb_delete(&(*bw)->wfb);
		}
		// adp is freed by the FileBlockWriter
		mem_free((void **)bw);
	} else {
		fatalError("bad ptr in wfbw_delete");
	}
}
*/

static void wfbw_set_complete(WF_BlockWrite *bw, SKOperationState::SKOperationState result) {
	if (bw->result != SKOperationState::INCOMPLETE) {
		srfsLog(LOG_WARNING, "Ignoring multiple completion in wfbw_set_complete %llx %d %d", bw, bw->result, result);
	} else {
		bw->result = result;
		if (bw->wfb != NULL && result == SKOperationState::SUCCEEDED) {
			if (srfsLogLevelMet(LOG_FINE)) {
				srfsLog(LOG_FINE, "bw %llx bw->wfb %llx &bw->wfb %llx", bw, bw->wfb, &bw->wfb);
			}
			wfb_delete(&bw->wfb);
		}
		// adp is freed by the FileBlockWriter
	}
}

void wf_sanityCheckNumBlocks(WritableFile *wf, char *file, int line) {
	srfsLog(LOG_FINE, "wf->blockList->size %u wf->blockList->numBlocks %d %s %d", wf->blockList->size, wf->blockList->numBlocks, file, line);
	if (wf->blockList->numBlocks == 0) {
		fatalError("wf->blockList->numBlocks == 0", file, line);
	}
}

WritableFile *wf_new(const char *path, mode_t mode, HashTableAndLock *htl,
                     AttrWriter *aw, FileAttr *fa, PartialBlockReader *pbr) {
	WritableFile	*wf;
    struct timespec tp;
	struct fuse_context	*fuseContext;	
	SKOperationState::SKOperationState	awResult;
    time_t  curEpochTimeSeconds;
    long    curTimeNanos;
	pthread_mutexattr_t mutexAttr;
    uint64_t    minModificationTimeMicros;
    
	fuseContext = fuse_get_context();

	wf = (WritableFile *)mem_alloc(1, sizeof(WritableFile));
	if (srfsLogLevelMet(LOG_FINE)) {
		srfsLog(LOG_FINE, "wf_new:\t%s %llx", path, wf);
	}
	
	wf->magic = WF_MAGIC;
    wf->path = str_dup(path);
    wf->htl = htl;
    
    if (clock_gettime(CLOCK_REALTIME, &tp)) {
        fatalError("clock_gettime failed", __FILE__, __LINE__);
    }    
    curEpochTimeSeconds = tp.tv_sec;
    curTimeNanos = tp.tv_nsec;
    
    if (fa == NULL) {            
        fid_generate_and_init_skfs(&wf->fa.fid);
        wf->fa.stat.st_mode = mode;
        wf->fa.stat.st_nlink = 1;
        wf->fa.stat.st_ino = fid_get_inode(&wf->fa.fid);
        //curEpochTimeSeconds = epoch_time_seconds();
        wf->fa.stat.st_ctime = curEpochTimeSeconds;
        wf->fa.stat.st_ctim.tv_nsec = curTimeNanos;

        wf->fa.stat.st_uid = fuseContext->uid;
        wf->fa.stat.st_gid = fuseContext->gid;
        wf->fa.stat.st_blksize = SRFS_BLOCK_SIZE;
        wf_new_block(wf);
        wf->blockList = abl_new(WF_INITIAL_BLOCK_SIZE, WF_MAX_BLOCK_SIZE, WF_BLOCK_INCREMENT, TRUE);
        wf_sanityCheckNumBlocks(wf, __FILE__, __LINE__);
    } else {
        memcpy(&wf->fa, fa, sizeof(FileAttr));
        wf->numBlocks = wf->fa.stat.st_size / SRFS_BLOCK_SIZE + 1;
        
        if (wf_init_cur_block(wf, pbr) != 0) {
            return NULL; // FIXME - FREE RESOURCES...
        }
        wf->blockList = abl_new(WF_INITIAL_BLOCK_SIZE, WF_MAX_BLOCK_SIZE, WF_BLOCK_INCREMENT, TRUE);
        wf_init_blockList(wf, wf->numBlocks - 1);
        wf->leastIncompleteBlockIndex = wf->numBlocks - 1;
    }
    wf->fa.stat.st_mtime = curEpochTimeSeconds;
    wf->fa.stat.st_atime = curEpochTimeSeconds;
    wf->fa.stat.st_mtim.tv_nsec = curTimeNanos;
    wf->fa.stat.st_atim.tv_nsec = curTimeNanos;
    
	pthread_mutexattr_init(&mutexAttr);
	pthread_mutexattr_settype(&mutexAttr, PTHREAD_MUTEX_RECURSIVE);	
    pthread_mutex_init(&wf->lock, &mutexAttr); 

    minModificationTimeMicros = stat_mtime_micros(&wf->fa.stat);
    
	if (ar_store_attr_in_cache_static((char *)wf->path, &wf->fa, TRUE, minModificationTimeMicros,
            SKFS_DEF_ATTR_TIMEOUT_SECS * 1000) != CACHE_STORE_SUCCESS) {
		wf_delete(&wf); // sets wf to NULL
	}
	
	/*	
	awResult = aw_write_attr_direct(aw, wf->path, &wf->fa, NULL, WF_ATTR_WRITE_MAX_ATTEMPTS);
	
	if (awResult != SKOperationState::SUCCEEDED) {
		//result = EIO;
		srfsLog(LOG_ERROR, "Couldn't write attribute for %s due to %d", path, awResult);
		wf_delete(&wf); // sets wf to NULL
	}
	*/
	return wf;
}

static int wf_init_cur_block(WritableFile *wf, PartialBlockReader *pbr) {
    size_t  curBlockLength;
    
    wf->curBlock = wfb_new();
    curBlockLength = wf->fa.stat.st_size % SRFS_BLOCK_SIZE;
    if (curBlockLength > 0) {
        int     readResult;
        char    *blockBuf;

        // Read in the new current block data
        blockBuf = (char *)mem_alloc(curBlockLength, 1);
        readResult = pbr_read_given_attr(pbr, wf->path, blockBuf, 
                                         curBlockLength, wf_cur_block_index(wf) * SRFS_BLOCK_SIZE, 
                                         &wf->fa, TRUE, 0);
        
        if (readResult != (int)curBlockLength) {
            mem_free((void **)&blockBuf);
            return -EIO;
        }
        wfb_write(wf->curBlock, blockBuf, curBlockLength);
        mem_free((void **)&blockBuf);
    }
    return 0;
}

static void wf_init_blockList(WritableFile *wf, size_t size) {
    size_t  i;
    
    for (i = 0; i < size; i++) {
        abl_add(wf->blockList, NULL);
    }
}

void wf_delete(WritableFile **wf) {
	if (wf != NULL && *wf != NULL) {
        srfsLog(LOG_FINE, "wf_delete %llx %llx", wf, *wf);
		(*wf)->magic = 0;
		abl_delete(&(*wf)->blockList);
        mem_free((void **)&(*wf)->path);
        if ((*wf)->pendingRename != NULL) {
            mem_free((void **)&(*wf)->pendingRename);
        }
        if ((*wf)->curBlock != NULL) {
            wfb_delete(&(*wf)->curBlock);
        }
		pthread_mutex_destroy(&(*wf)->lock);
		mem_free((void **)wf);
	} else {
		fatalError("bad ptr in wf_delete");
	}
}

void wf_set_parent_dir(WritableFile *wf, OpenDir *parentDir, uint64_t parentDirUpdateTimeMillis) {
    pthread_mutex_lock(&wf->lock);    
    if (wf->parentDir == NULL) {
        wf->parentDir = parentDir;
        wf->parentDirUpdateTimeMillis = parentDirUpdateTimeMillis;
    }
    pthread_mutex_unlock(&wf->lock);    
}

WritableFileReference *wf_add_reference(WritableFile *wf, char *file, int line) {
    return wfr_new(wf, file, line);
}

static void wf_new_block(WritableFile *wf) {
	WritableFileBlock    *newBlock;
	
	newBlock = wfb_new();
	wf->curBlock = newBlock;
	wf->numBlocks++;
}

void wf_set_pending_rename(WritableFile *wf, const char *newName) {
    srfsLog(LOG_WARNING, "wf_set_pending_rename %s --> %s", wf->path, newName);
    pthread_mutex_lock(&wf->lock);    
    if (wf->pendingRename != NULL) {
        mem_free((void **)&wf->pendingRename);
    }
    wf->pendingRename = str_dup(newName);
    pthread_mutex_unlock(&wf->lock);
}

// lock must be held
static void wf_skip_ahead(WritableFile *wf, off_t writeOffset, FileBlockWriter *fbw) {
    uint64_t    newBlockIndex;
    size_t      newBlockOffset;
    size_t      bytesToWrite;
    size_t      bytesWritten;
    
    newBlockIndex = writeOffset / SRFS_BLOCK_SIZE;
    if (newBlockIndex > wf_cur_block_index(wf)) { // Skip beyond current block
        uint64_t    zbIndex;
        
        // Zero out the remainder of the current block, and write it
        wf->fa.stat.st_size += wfb_zero_out_remainder(wf->curBlock);
		wf_write_block(wf, wf->curBlock, wf_cur_block_index(wf), fbw);
        // Write zero blocks (reader will interpret these as full blocks of zeros)
        for (zbIndex = wf->numBlocks; zbIndex < newBlockIndex; zbIndex++) {
			wf_write_block(wf, wfb_new(), zbIndex, fbw);
            wf->numBlocks++;
            wf->fa.stat.st_size += SRFS_BLOCK_SIZE;
        }
        // Create new current block
        wf->curBlock = wfb_new();
        wf->numBlocks++;
        if (wf->numBlocks != newBlockIndex + 1) {
            fatalError("wf->numBlocks != newBlockIndex + 1", __FILE__, __LINE__);
        }
    }
    // Now skip within the current block
    newBlockOffset = writeOffset % SRFS_BLOCK_SIZE;
    bytesToWrite = newBlockOffset - wf->curBlock->size;
    bytesWritten = wfb_write(wf->curBlock, (const char *)zeroBlock, bytesToWrite);
    if (bytesWritten != bytesToWrite) {
        fatalError("bytesWritten != bytesToWrite", __FILE__, __LINE__);
    }
    wf->fa.stat.st_size += bytesToWrite;
}

static size_t wf_rewrite_past_blocks(WritableFile *wf, const char *src, size_t pastWriteSize, off_t writeOffset, 
            uint64_t firstBlockIndex, uint64_t lastPastBlockIndex,
            FileBlockWriter *fbw, PartialBlockReader *pbr, FileBlockCache *fbc) {
    uint64_t    numBlocks;
    size_t      rewriteBufSize;
    char *      rewriteBuf;
    int         readResult;
    off_t       firstBlockOffset;
    
    srfsLog(LOG_FINE, "wf_rewrite_past_blocks %llx %s %u %u %u %u", wf, src, pastWriteSize, writeOffset, firstBlockIndex, lastPastBlockIndex);
    // Read in blocks
    numBlocks = lastPastBlockIndex - firstBlockIndex + 1;
    rewriteBufSize = numBlocks * SRFS_BLOCK_SIZE;
    rewriteBuf = (char *)mem_alloc(rewriteBufSize, 1);
    readResult = pbr_read_given_attr(pbr, wf->path, rewriteBuf, rewriteBufSize, firstBlockIndex * SRFS_BLOCK_SIZE, &wf->fa, TRUE, 0);
    
    // Rewrite
    firstBlockOffset = writeOffset % SRFS_BLOCK_SIZE;
    memcpy(rewriteBuf + firstBlockOffset, src, pastWriteSize);
    srfsLog(LOG_FINE, "firstBlockOffset %u", firstBlockOffset);
    
    // Write back modified blocks
    uint64_t    i;
    uint64_t    blockWriteTimeMillis;
    
    blockWriteTimeMillis = curTimeMillis();
    for (i = 0; i < numBlocks; i++) {
        size_t  bytesWritten;
        WritableFileBlock   *wfb;
        CacheStoreResult    blockCacheStoreResult;
        FileBlockID	*fbid;
        void    *cacheBlock;
        
        wfb = wfb_new();
        bytesWritten = wfb_write(wfb, rewriteBuf + SRFS_BLOCK_SIZE * i, SRFS_BLOCK_SIZE);
        if (bytesWritten != SRFS_BLOCK_SIZE) {
            fatalError("bytesWritten != SRFS_BLOCK_SIZE", __FILE__, __LINE__);
        }
        srfsLog(LOG_FINE, "past wf_write_block %llx %llx %u", wf, wfb, (firstBlockIndex + i));
        cacheBlock = mem_dup(wfb->block, wfb->size);
        wf_write_block_sync(wf, wfb, firstBlockIndex + i, fbw);
        fbid = fbid_new(&wf->fa.fid, (firstBlockIndex + i));
        blockCacheStoreResult = fbc_store_raw_data(fbc, fbid, cacheBlock, wfb->size, TRUE,
                                        blockWriteTimeMillis * 1000);
        if (blockCacheStoreResult != CACHE_STORE_SUCCESS) {
            srfsLog(LOG_ERROR, "wf_rewrite_past_blocks blockCacheStoreResult != CACHE_STORE_SUCCESS %s", wf->path);
            mem_free((void **)&cacheBlock);
        }
        fbid_delete(&fbid);
        wfb_delete(&wfb);
    }
    mem_free((void **)&rewriteBuf);
    return pastWriteSize;
}

// lock must be held
static size_t wf_rewrite(WritableFile *wf, const char *src, size_t writeSize, off_t writeOffset, 
             FileBlockWriter *fbw, PartialBlockReader *pbr, FileBlockCache *fbc) {
    uint64_t    firstBlockIndex;
    uint64_t    lastBlockIndex;
    size_t      bytesWritten;
    size_t      rewriteSize;
    size_t      pastRewriteSize;
    size_t      curBlockRewriteOffset;
    size_t      curBlockRewriteSize;
    size_t      totalWritten;
    size_t      curBlockSrcOffset;
    
    srfsLog(LOG_FINE, "wf_rewrite %llx %s %u %u", wf, src, writeSize, writeOffset);
    // first flush all new blocks
	wf_limit_outstanding_blocks(wf, fbw, 0);
    
    if (writeOffset + (off_t)writeSize >= wf->fa.stat.st_size) {
        rewriteSize = wf->fa.stat.st_size - writeOffset;
    } else {
        rewriteSize = writeSize;
    }
    firstBlockIndex = offsetToBlock(writeOffset);
    lastBlockIndex = offsetToBlock(writeOffset + rewriteSize - 1);
    totalWritten = 0;

    // Current block rewrite
    if (lastBlockIndex == wf_cur_block_index(wf)) {
        srfsLog(LOG_FINE, "rewrite current block");
        if ((size_t)writeOffset < lastBlockIndex * SRFS_BLOCK_SIZE) {
            curBlockRewriteOffset = 0;
            curBlockRewriteSize = (writeOffset + rewriteSize) % SRFS_BLOCK_SIZE;
        } else {
            curBlockRewriteOffset = writeOffset % SRFS_BLOCK_SIZE;
            curBlockRewriteSize = rewriteSize;
        }
        // This is a rewrite; we shouldn't be exceeding the current block size
        if (curBlockRewriteOffset + curBlockRewriteSize > wf->curBlock->size) {
            fatalError("lastBlockIndex > wf_cur_block_index(wf)", __FILE__, __LINE__);
        }
        curBlockSrcOffset = writeSize - curBlockRewriteSize; // compute location of src for current block
        totalWritten += wfb_rewrite(wf->curBlock, src + curBlockSrcOffset, curBlockRewriteOffset, curBlockRewriteSize);
        srfsLog(LOG_FINE, "%u", totalWritten);
    } else {
        srfsLog(LOG_FINE, "no rewrite current block");
        if (lastBlockIndex > wf_cur_block_index(wf)) {
            fatalError("lastBlockIndex > wf_cur_block_index(wf)", __FILE__, __LINE__);
        }
    }
    
    // Past blocks rewrite
    if (firstBlockIndex < wf_cur_block_index(wf)) {
        uint64_t    lastPastBlockIndex;
        size_t      pastWriteSize;
        
        srfsLog(LOG_FINE, "rewrite past blocks");
        lastPastBlockIndex = uint64_min(lastBlockIndex, wf_cur_block_index(wf) - 1);
        pastWriteSize = off_min(writeOffset + writeSize, wf_cur_block_index(wf) * SRFS_BLOCK_SIZE) 
                        - writeOffset;
        totalWritten += wf_rewrite_past_blocks(wf, src, pastWriteSize, writeOffset, 
                                                firstBlockIndex, lastPastBlockIndex,
                                                fbw, pbr, fbc);
        srfsLog(LOG_FINE, "%u", totalWritten);
    } else {
        srfsLog(LOG_FINE, "no rewrite past blocks");
    }
    
    return rewriteSize;
}

int wf_write(WritableFile *wf, const char *src, size_t writeSize, off_t writeOffset, 
             FileBlockWriter *fbw, PartialBlockReader *pbr, FileBlockCache *fbc) {
    size_t  totalBytesWritten;
    size_t  rewriteSize;
    
	if (srfsLogLevelMet(LOG_FINE)) {
		srfsLog(LOG_FINE, "wf_write %lx %lx %u %lu %lx", wf, (uint64_t)src, writeSize, writeOffset, fbw);
	}
    totalBytesWritten = 0;
    rewriteSize = 0;
    pthread_mutex_lock(&wf->lock);
	if (writeOffset < wf->fa.stat.st_size) {
		srfsLog(LOG_FINE, "writeOffset < wf->fa.stat.st_size %u %u", writeOffset, wf->fa.stat.st_size);
        rewriteSize = wf_rewrite(wf, src, writeSize, writeOffset, fbw, pbr, fbc);
        if (rewriteSize > writeSize) {
            fatalError("rewriteSize > writeSize", __FILE__, __LINE__);
        }
        if (rewriteSize == writeSize) {
            // This was purely a rewrite; return
            pthread_mutex_unlock(&wf->lock);
            srfsLog(LOG_FINE, "writeSize.a %u", writeSize);
            return writeSize;
        } else {
            // Fall through to handle non-rewrite portion
            writeOffset += rewriteSize;
            totalBytesWritten = rewriteSize;
        }
	} else {
        if (writeOffset > wf->fa.stat.st_size) {
            wf_skip_ahead(wf, writeOffset, fbw);
        }
    }
    do {
        size_t  remaining;
        size_t  bytesWritten;
    
        remaining = writeSize - totalBytesWritten;
        bytesWritten = wfb_write(wf->curBlock, src + totalBytesWritten, remaining);
        totalBytesWritten += bytesWritten;
        if (wfb_is_full(wf->curBlock)) {
            wf_limit_outstanding_blocks(wf, fbw, WF_MAX_BLOCKS_OUTSTANDING);
            wf_write_block(wf, wf->curBlock, wf_cur_block_index(wf), fbw);
            wf_new_block(wf);
        }
    } while (totalBytesWritten < writeSize);		
    wf->fa.stat.st_size += writeSize - rewriteSize;
    pthread_mutex_unlock(&wf->lock);
    srfsLog(LOG_FINE, "writeSize.b %u", writeSize);
    return writeSize;
}

// lock must be held
static int _wf_truncate(WritableFile *wf, off_t size, FileBlockWriter *fbw, PartialBlockReader *pbr) {
    uint64_t    newCurBlockIndex;
    size_t      newCurBlockLength;
    size_t      newNumBlocks;
    
    // Note that after we truncate, cached file blocks could be invalid. 
    // This is handled by the FileBlockCache support for the modification time.
    // The newly stored attribute will tell us to user newer blocks, and the
    // old cached blocks will be dropped.
    
    newCurBlockIndex = size / SRFS_BLOCK_SIZE;
    newCurBlockLength = size % SRFS_BLOCK_SIZE;
    newNumBlocks = newCurBlockIndex + 1;
    if (newCurBlockIndex > wf_cur_block_index(wf)) {
        fatalError("panic", __FILE__, __LINE__); // preconditions should make this impossible
    } else if (newCurBlockIndex < wf_cur_block_index(wf)) { // Shrinking blocks
        // Flush any outstanding blocks
        wf_limit_outstanding_blocks(wf, fbw, 0);
        // Abandon the current block
        wfb_delete(&wf->curBlock);
        wf->curBlock = wfb_new();
        // Adjust list size (to remove the old write)
        abl_truncate(wf->blockList, newNumBlocks - 1);
    
        if (newCurBlockLength > 0) {
            int     readResult;
            char    *blockBuf;

            // Read in the new current block data
            // Any block < current block must be a full block, so read a full block
            // Even though we might not actually care about all of it
            blockBuf = (char *)mem_alloc(SRFS_BLOCK_SIZE, 1);
            readResult = pbr_read_given_attr(pbr, wf->path, blockBuf, 
                                             SRFS_BLOCK_SIZE, newCurBlockIndex * SRFS_BLOCK_SIZE, &wf->fa, TRUE, 0);
            if (readResult != 0) {
                return -EIO;
            }
            // write the portion that we care about from the temp buf to the new block
            wfb_write(wf->curBlock, blockBuf, newCurBlockLength);
            mem_free((void **)&blockBuf);
        }
        wf->numBlocks = newNumBlocks;
        wf->leastIncompleteBlockIndex = wf->numBlocks - 1;
        wf->fa.stat.st_blocks = statBlockConversion(wf->numBlocks); 
                                                // FUTURE - we don't normally update this on the fly
                                                // maybe change that everywhere        
    } else { // Modify the current block size only
        // Truncate the current block
        wfb_truncate(wf->curBlock, newCurBlockLength);
    }
    // set the size
    wf->fa.stat.st_size = size;
    return 0;
}

int wf_truncate(WritableFile *wf, off_t size, FileBlockWriter *fbw, PartialBlockReader *pbr) {
    int rc;
    
    pthread_mutex_lock(&wf->lock);    
    if (size < wf->fa.stat.st_size) { // reduction
        srfsLog(LOG_INFO, "truncate: size < wf->fa.stat.st_size");
        rc = _wf_truncate(wf, size, fbw, pbr);
    } else if (size == wf->fa.stat.st_size) { // no change
        rc = 0;
    } else { // extension
        wf_skip_ahead(wf, size, fbw); 
        rc = 0;
    }
    pthread_mutex_unlock(&wf->lock);
    return rc;
}

int wf_flush(WritableFile *wf, AttrWriter *aw, FileBlockWriter *fbw, AttrCache *ac) {
    int rc;
    
    pthread_mutex_lock(&wf->lock);    
    rc = _wf_flush(wf, aw, fbw, ac);
    pthread_mutex_unlock(&wf->lock);
    return rc;
}

// lock must be held
static int _wf_flush(WritableFile *wf, AttrWriter *aw, FileBlockWriter *fbw, AttrCache *ac) {
	int	blocksOK;
    int result;
	
	srfsLog(LOG_FINE, "in wf_flush");
	srfsLog(LOG_FINE, "wf_flush. wait for writes to complete");
    result = 0;
    blocksOK = TRUE;
	wf_sanityCheckNumBlocks(wf, __FILE__, __LINE__);
	// Wait for all block writes to complete
	wf_limit_outstanding_blocks(wf, fbw, 0);
	srfsLog(LOG_FINE, "wf_flush. ensure blocks written successfully");
    blocksOK = wf_blocks_written_successfully(wf, abl_size(wf->blockList), fbw);
    // FUTURE - consider retrying failed blocks
	if (!wfb_is_empty(wf->curBlock)) { // attempt this write even if others failed
        SKOperationState::SKOperationState	bwResult;
        
		bwResult = wf_write_block_sync(wf, wf->curBlock, wf_cur_block_index(wf), fbw);    
        if (bwResult != SKOperationState::SUCCEEDED) {
            blocksOK = FALSE;
        }
    }
    if (blocksOK) {
        //if (wf->kvAttrStale) {
            result = wf_update_attr(wf, aw, ac, FALSE);
        //}
        if (wf_syncDirUpdates && wf->parentDir != NULL) {
            srfsLog(LOG_INFO, "_wf_flush %s od_waitForWrite parentDir %s", wf->path, wf->parentDir->path);
            od_waitForWrite(wf->parentDir, wf->parentDirUpdateTimeMillis);
            srfsLog(LOG_INFO, "_wf_flush %s od_waitForWrite parentDir %s complete", wf->path, wf->parentDir->path);
        } else {
            srfsLog(LOG_INFO, "No parentDir set %s", wf->path);
        }
    } else {
        if (WF_TRUNCATE_ON_FLUSH_ERROR) {
            off_t   okBytes;
            int     tResult;
            int     aResult;
            
            // In case of an error, we make a best-effort attempt
            // to truncate the file to the last successfully written block
            srfsLog(LOG_WARNING, "Error in _wf_flush %s. Attempting to truncate file and update attribute.", wf->path);
            okBytes = wf_bytes_successfully_written(wf, fbw);
            srfsLog(LOG_WARNING, "%s okBytes %d", wf->path, okBytes);
            tResult = _wf_truncate(wf, okBytes, fbw, NULL); // NULL is allowed for pbr since we won't have any partial blocks to read
            srfsLog(LOG_WARNING, "%s _wf_truncate %s", wf->path, !tResult ? "succeeded" : "failed");
            aResult = wf_update_attr(wf, aw, ac, FALSE);
            srfsLog(LOG_WARNING, "%s wf_update_attr %s", wf->path, !aResult ? "succeeded" : "failed");
        }
        result = EIO;
    }
	srfsLog(LOG_FINE, "out wf_flush %d", result);
    return result;
}

static int wf_update_attr(WritableFile *wf, AttrWriter *aw, AttrCache *ac, int cacheOnly) {
    SKOperationState::SKOperationState	awResult;
	time_t	curTimeSeconds;
    long    curTimeNanos;
    struct timespec tp;
    int result;
    
    wf->kvAttrStale = FALSE;
    result = 0;
    // Fill in file stat information
    if (clock_gettime(CLOCK_REALTIME, &tp)) {
        fatalError("clock_gettime failed", __FILE__, __LINE__);
    }    
    curTimeSeconds = tp.tv_sec;
    curTimeNanos = tp.tv_nsec;
    wf->fa.stat.st_mtime = curTimeSeconds;
    wf->fa.stat.st_atime = curTimeSeconds;
    wf->fa.stat.st_mtim.tv_nsec = curTimeNanos;
    wf->fa.stat.st_atim.tv_nsec = curTimeNanos;
	if (!wfb_is_empty(wf->curBlock)) {
        wf->fa.stat.st_blocks = statBlockConversion(wf->numBlocks);
    } else {
        wf->fa.stat.st_blocks = statBlockConversion(wf->numBlocks - 1);
    }
    
    if (cacheOnly) {
		CacheStoreResult	acResult;
		
		acResult = ac_store_raw_data(ac, (char *)wf->path, fa_dup(&wf->fa), TRUE, 
                        curTimeMicros(), SKFS_DEF_ATTR_TIMEOUT_SECS * 1000);
		if (acResult != CACHE_STORE_SUCCESS) {
			srfsLog(LOG_ERROR, "ac_store_raw_data_failed with %d at %s %d", acResult, __FILE__, __LINE__);
			result = EIO;
		}
    } else {
        //aw_write_attr(aw, wf->path, &wf->fa); // old queued async write
        // Write out attribute information & wait for the write to complete
        awResult = aw_write_attr_direct(aw, wf->path, &wf->fa, ac, WF_ATTR_WRITE_MAX_ATTEMPTS);
        srfsLog(LOG_FINE, "wf_update_attr %s aw_write result %d\n", wf->path, awResult);
        if (awResult != SKOperationState::SUCCEEDED) {
            result = EIO;
        }    
    }
   
    return result;
}

// called only from wf_check_for_close()
static int wf_close(WritableFile *wf, AttrWriter *aw, FileBlockWriter *fbw, AttrCache *ac) {
	int		result;

	result = 0;
	srfsLog(LOG_FINE, "in wf_close %s", wf->path);
    pthread_mutex_lock(&wf->lock);    
    
    // wf_flush() only updates the cached attribute. Update the the key-value store.
    // UPDATE: wf_flush() is now updating both the cached attribute and the key-value store
    // Assumption: fuse is calling flush() after every OS close() operation
	//result = wf_update_attr(wf, aw, NULL);
    wfb_delete(&wf->curBlock);
    
    pthread_mutex_unlock(&wf->lock);
	srfsLog(LOG_FINE, "leaving wf_close %s", wf->path);
	wf_delete(&wf);
	srfsLog(LOG_FINE, "out wf_close");
    return result;
}

static SKOperationState::SKOperationState wf_write_block_sync(WritableFile *wf, WritableFileBlock *wfb, uint64_t blockIndex, FileBlockWriter *fbw, int maxAttempts) {
	FileBlockID	*fbid;
	FBW_ActiveDirectPut	*adp;
	SKOperationState::SKOperationState	result;
    int attempt;
	
    if (wfb == NULL) {
        srfsLog(LOG_WARNING, "NULL wfb %s %d", wf->path, blockIndex);
        return SKOperationState::FAILED;
    }
    attempt = 1;
    wf->kvAttrStale = TRUE;
	if (srfsLogLevelMet(LOG_FINE)) {
		srfsLog(LOG_FINE, "wf_rewrite_block %u", wfb->size);
	}
	fbid = fbid_new(&wf->fa.fid, blockIndex);
    do {
        adp = fbw_put_direct(fbw, fbid, wfb);
        result = fbw_wait_for_direct_put(fbw, &adp);
        if (result != SKOperationState::SUCCEEDED) {        
            char    key[SRFS_FBID_KEY_SIZE];
            
            fbid_to_string(fbid, key);            
            srfsLog(LOG_ERROR, "wf_write_block_sync failed %s %d %s. Attempt %d of %d", wf->path, result, key, attempt, maxAttempts);
        }
    } while (result != SKOperationState::SUCCEEDED && attempt++ < maxAttempts);
	fbid_delete(&fbid);
    return result;
}

static void wf_write_block(WritableFile *wf, WritableFileBlock *wfb, uint64_t blockIndex, FileBlockWriter *fbw) {
	FileBlockID	*fbid;
	FBW_ActiveDirectPut	*adp;
	
    wf->kvAttrStale = TRUE;
	if (srfsLogLevelMet(LOG_FINE)) {
		srfsLog(LOG_FINE, "wf_write_block %u %u", blockIndex, wfb->size);
	}
	fbid = fbid_new(&wf->fa.fid, blockIndex);
	adp = fbw_put_direct(fbw, fbid, wfb);
	abl_add(wf->blockList, wfbw_new(wfb, adp));
	fbid_delete(&fbid);
}

WritableFile *wf_fuse_fi_fh_to_wf(struct fuse_file_info *fi) {
    SKFSOpenFile    *sof;
    WritableFile    *wf;
	
    sof = (SKFSOpenFile*)fi->fh;
    if (!sof_is_valid(sof)) {
        srfsLog(LOG_ERROR, "Bogus fi->fh");
        wf = NULL;
    } else {
        WritableFileReference   *wf_ref;
        
        wf_ref = (WritableFileReference *)sof->wf_ref;
        wf = wfr_get_wf(wf_ref);
        if (wf != NULL) {
            if (wf->magic != WF_MAGIC) {
                srfsLog(LOG_ERROR, "Bogus wfr in sof. wf->magic is %x", wf->magic);			
                wf = NULL;
            }
        }
    }
	return wf;
}

// number of blocks written, but not known to have completed
// this does not count unwritten blocks
static uint64_t wf_blocks_outstanding(WritableFile *wf) {
	return abl_size(wf->blockList) - wf->leastIncompleteBlockIndex;
}

static uint64_t wf_cur_block_index(WritableFile *wf) {
    return wf->numBlocks - 1;
}

// lock must be held
static void wf_limit_outstanding_blocks(WritableFile *wf, FileBlockWriter *fbw, uint64_t limit) {
	while (wf_blocks_outstanding(wf) > limit) {
		SKOperationState::SKOperationState	result;
		WF_BlockWrite	*bw;
	
		bw = (WF_BlockWrite *)abl_get(wf->blockList, wf->leastIncompleteBlockIndex);
		if (bw == NULL) {
			fatalError("Unexpected bw == NULL", __FILE__, __LINE__);
        }
		if (bw->result != SKOperationState::INCOMPLETE) {
			fatalError("Unexpected complete operation", __FILE__, __LINE__);
		}
		if (bw->adp == NULL) {
			fatalError("Unexpected null adp", __FILE__, __LINE__);
		}
		result = fbw_wait_for_direct_put(fbw, &bw->adp);
		wfbw_set_complete(bw, result);
		wf->leastIncompleteBlockIndex++;
	}
}

// Verifies that all blocks that have been written to SK have succeeded.
// Does not consider blocks that have not yet been written to SK.
static int wf_all_blocks_written_successfully(WritableFile *wf, FileBlockWriter *fbw) {
	return wf_blocks_written_successfully(wf, wf->numBlocks, fbw);
}

static int wf_blocks_written_successfully(WritableFile *wf, size_t numBlocks, FileBlockWriter *fbw) {
	size_t	i;
	
	for (i = 0; i < numBlocks; i++) {
		WF_BlockWrite	*bw;
		
		bw = (WF_BlockWrite *)abl_get(wf->blockList, i);
		if (bw != NULL && bw->result != SKOperationState::SUCCEEDED) {
            SKOperationState::SKOperationState  retryResult;
            char	    key[SRFS_FBID_KEY_SIZE];
            FileBlockID	*fbid;
            
            fbid = fbid_new(&wf->fa.fid, i);
            fbid_to_string(fbid, key);            
            fbid_delete(&fbid);
			srfsLog(LOG_WARNING, "Block not successful: %s %d %s %d", wf->path, i, key, bw->result);
            retryResult = wf_write_block_sync(wf, bw->wfb, i, fbw, WF_FAILED_BLOCK_RETRIES);
            if (retryResult == SKOperationState::SUCCEEDED) {
                srfsLog(LOG_WARNING, "Block retried successfully: %s %d %s %d", wf->path, i, key, bw->result);
                bw->result = SKOperationState::SUCCEEDED;
            } else {
                if (bw->wfb != NULL) {
                    wfb_delete(&bw->wfb);
                }
                return FALSE;
            }
		}
	}
	return TRUE;
}

static off_t wf_bytes_successfully_written(WritableFile *wf, FileBlockWriter *fbw) {
	size_t	i;
	
	for (i = 0; i < wf->numBlocks; i++) {
		WF_BlockWrite	*bw;
		
		bw = (WF_BlockWrite *)abl_get(wf->blockList, i);
		if (bw != NULL && bw->result != SKOperationState::SUCCEEDED) {
            SKOperationState::SKOperationState  retryResult;
            char	    key[SRFS_FBID_KEY_SIZE];
            FileBlockID	*fbid;
            
            fbid = fbid_new(&wf->fa.fid, i);
            fbid_to_string(fbid, key);            
            fbid_delete(&fbid);
			srfsLog(LOG_WARNING, "Block not successful: %s %d %s %d", wf->path, i, key, bw->result);
            retryResult = wf_write_block_sync(wf, bw->wfb, i, fbw, WF_FAILED_BLOCK_RETRIES);
            if (retryResult == SKOperationState::SUCCEEDED) {
                srfsLog(LOG_WARNING, "Block retried successfully: %s %d %s %d", wf->path, i, key, bw->result);
                bw->result = SKOperationState::SUCCEEDED;
            } else {
                return i * SRFS_BLOCK_SIZE;
            }
		}
	}
	return wf->fa.stat.st_size;
}

int wf_modify_attr(WritableFile *wf, mode_t *mode, uid_t *uid, gid_t *gid, 
                    const struct timespec *last_access_tp, const struct timespec *last_modification_tp, const struct timespec *last_change_tp) {
    // FUTURE - check on semantics of allowing all
    pthread_mutex_lock(&wf->lock);
    if (mode != NULL) {
        wf->fa.stat.st_mode = *mode;
    }
    if (uid != NULL && *uid != (gid_t)-1) {
        wf->fa.stat.st_uid = *uid;
    }
    if (gid != NULL && *gid != (gid_t)-1) {
        wf->fa.stat.st_gid = *gid;
    }

    if (last_access_tp != NULL) {
        wf->fa.stat.st_atime = last_access_tp->tv_sec;
        wf->fa.stat.st_atim.tv_nsec = last_access_tp->tv_nsec;
    }    
    if (last_modification_tp != NULL) {
        wf->fa.stat.st_mtime = last_modification_tp->tv_sec;
        wf->fa.stat.st_mtim.tv_nsec = last_modification_tp->tv_nsec;
    }    
    if (last_change_tp != NULL) {
        wf->fa.stat.st_ctime = last_change_tp->tv_sec;
        wf->fa.stat.st_ctim.tv_nsec = last_change_tp->tv_nsec;
    }    
    pthread_mutex_unlock(&wf->lock);
    
    return 0;
}

//////////////////////////////
// reference code

int wf_create_ref(WritableFile *wf) {
	int	ref;

	ref = -1;
	pthread_mutex_lock(&wf->lock);
    
    if (wf->referentState.nextRef >= WFR_RECYCLE_THRESHOLD) {
        ref = _wf_find_empty_ref(wf);
    }
    if (ref >= 0) {
        wf->referentState.refStatus[ref] = WFR_Created;
    } else {
        if (wf->referentState.nextRef < WFR_MAX_REFS) {
            ref = wf->referentState.nextRef++;
            if (wf->referentState.refStatus[ref] != WFR_Invalid) {
                fatalError("wf->referentState.refStatus[ref] != WFR_Invalid", __FILE__, __LINE__);
            }
            wf->referentState.refStatus[ref] = WFR_Created;
        } else {
            fatalError("WFR_MAX_REFS exceeded", __FILE__, __LINE__);
        }
    }
    
	pthread_mutex_unlock(&wf->lock);
	return ref;
}

// lock must be held
static int _wf_find_empty_ref(WritableFile *wf) {
	int	i;

    for (i = 0; i < wf->referentState.nextRef; i++) {
        if (wf->referentState.refStatus[i] != WFR_Created) {
            return i;
        }
    }
    return -1;
}

// lock must be held
static int _wf_has_references(WritableFile *wf) {
	int	noReferences;
	int	i;

	noReferences = TRUE;
    for (i = 0; i < wf->referentState.nextRef; i++) {
        if (wf->referentState.refStatus[i] != WFR_Destroyed) {
            noReferences = FALSE;
            break;
        }
    }
    return !noReferences;
}

/*
int wf_has_references(WritableFile *wf) {
    int hasReferences;
    
	pthread_mutex_lock(&wf->lock);
    hasReferences = _wf_has_references(wf);
	pthread_mutex_unlock(&wf->lock);
    return hasReferences;
}
*/

/*
static void wf_check_for_deletion(WritableFile *wf) {
	int	doDelete;
	int	i;

	doDelete = TRUE;
	//pthread_mutex_lock(&wf->lock); - this must already be held
	if (!wf->referentState.toDelete) {
		for (i = 0; i < wf->referentState.nextRef; i++) {
			if (wf->referentState.refStatus[i] != WFR_Destroyed) {
				doDelete = FALSE;
				break;
			}
		}
		if (doDelete) {
			wf->referentState.toDelete = TRUE;
		}
	} else {
		doDelete = FALSE;
	}
	pthread_mutex_unlock(&wf->lock);
	if (doDelete) {
		srfsLog(LOG_FINE, "All WritableFile references destroyed. Deleting %llx", wf);
		wf_delete(&wf);
	}
}
*/

// Only called by wf_delete_ref()
// file and table locks must be held
static int wf_check_for_close(WritableFile *wf, AttrWriter *aw, FileBlockWriter *fbw, AttrCache *ac) {
    int hasReferences;
    int rc;
    
    hasReferences = _wf_has_references(wf);
    if (!hasReferences) {
        int rc_f;
        int rc_c;
        WritableFile *_wf;
        
        wf_sanity_check(wf);
        _wf = (WritableFile *)hashtable_remove(wf->htl->ht, (void *)wf->path); 
        if (_wf != wf) {
            srfsLog(LOG_ERROR, "wf->htl->ht %llx wf->path %s _wf %llx wf %llx", 
                                wf->htl->ht, wf->path, _wf, wf);
            fatalError("_wf != wf", __FILE__, __LINE__);
        }
        // We would like to drop the table lock here to avoid holding it during remote i/o
        // We cannot, however, as this can introduce inconsistency.
        // (Mitigating this with many table partitions to minimize extraneous locking)
        rc_f = _wf_flush(wf, aw, fbw, ac);
        // Now we may drop the table lock safely.
        pthread_rwlock_unlock(&wf->htl->rwLock);
        // we removed wf from the table, so we no longer need a lock
        // hold it to here just to meet the documented lock contract of _wf_flush()
        pthread_mutex_unlock(&wf->lock);
        rc_c = wf_close(wf, aw, fbw, ac);
        if (rc_f != 0) {
            rc = rc_f;
        } else {
            rc = rc_c;
        }
    } else {
        pthread_mutex_unlock(&wf->lock);
        pthread_rwlock_unlock(&wf->htl->rwLock);
        rc = 0;
    }
    return rc;
}

// Only called by wfr_delete
int wf_delete_ref(WritableFile *wf, int ref, AttrWriter *aw, FileBlockWriter *fbw, AttrCache *ac) {
    int hasReferences;
    HashTableAndLock    *htl;
    int rc;

    htl = wf->htl;
    pthread_rwlock_wrlock(&htl->rwLock);    
	pthread_mutex_lock(&wf->lock);
	if (ref >= wf->referentState.nextRef) {
		fatalError("ref >= wf->referentState.nextRef", __FILE__, __LINE__);
	}
	if (wf->referentState.refStatus[ref] != WFR_Created) {
		fatalError("wf->referentState.refStatus[ref] != WFR_Created", __FILE__, __LINE__);
	}
	wf->referentState.refStatus[ref] = WFR_Destroyed;
    
    // Before locking the table partition [in wf_check_for_close()], 
    // check the file for references
    hasReferences = _wf_has_references(wf);
    if (!hasReferences) {
        rc = wf_check_for_close(wf, aw, fbw, ac);
    } else {
        pthread_mutex_unlock(&wf->lock);
        pthread_rwlock_unlock(&htl->rwLock);
        rc = 0;
    }
    return rc;
}

void wf_sanity_check(WritableFile *wf) {
    if (wf == NULL) {
        fatalError("Unexpected null wf", __FILE__, __LINE__);
    }
    if (wf->magic != WF_MAGIC) {
        srfsLog(LOG_ERROR, "wf %llx wf->magic %x WF_MAGIC %x", wf, wf->magic, WF_MAGIC);
        fatalError("Unexpected wf->magic != WF_MAGIC", __FILE__, __LINE__);
    }
}
