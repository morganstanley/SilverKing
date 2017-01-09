// WritableFileBlock.c

/////////////
// includes

#include <stdlib.h>
#include <string.h>

#include "WritableFileBlock.h"
#include "Util.h"


///////////////////
// implementation

WritableFileBlock *wfb_new() {
	WritableFileBlock    *wfb;

	wfb = (WritableFileBlock *)mem_alloc(1, sizeof(WritableFileBlock));
    srfsLog(LOG_FINE, "wfb_new %llx\n", wfb);
	return wfb;
}

void wfb_delete(WritableFileBlock **wfb) {
	if (wfb != NULL && *wfb != NULL) {
		mem_free((void **)wfb);
	} else {
		fatalError("bad ptr in wfb_delete");
	}
}

size_t wfb_write(WritableFileBlock *wfb, const char *src, size_t length) {
	size_t  bytesToWrite;
	
	bytesToWrite = size_min(length, SRFS_BLOCK_SIZE - wfb->size);
	memcpy(wfb->block + wfb->size, src, bytesToWrite);
	wfb->size += bytesToWrite;
	return bytesToWrite;
}

size_t wfb_rewrite(WritableFileBlock *wfb, const char *src, size_t offset, size_t length) {
    srfsLog(LOG_FINE, "wfb_rewrite %llx %llx %u %u", wfb, src, offset, length);
    if (offset + length > wfb->size) {
        // This is a rewrite; we shouldn't be exceeding the current size
        fatalError("offset + length > wfb->size", __FILE__, __LINE__);
    }
	memcpy(wfb->block + offset, src, length);
	return length;
}

void wfb_truncate(WritableFileBlock *wfb, size_t length) {
    if (length > SRFS_BLOCK_SIZE) {
        fatalError("length > SRFS_BLOCK_SIZE", __FILE__, __LINE__);
    }
    wfb->size = length;
}

int wfb_is_full(WritableFileBlock *wfb) {
    return wfb->size == SRFS_BLOCK_SIZE;
}

int wfb_is_empty(WritableFileBlock *wfb) {
    return wfb->size == 0;
}

size_t wfb_zero_out_remainder(WritableFileBlock *wfb) {
	size_t  bytesToWrite;
	
	bytesToWrite = SRFS_BLOCK_SIZE - wfb->size;
	memcpy(wfb->block + wfb->size, zeroBlock, bytesToWrite);
	wfb->size += bytesToWrite;
    return bytesToWrite;
}
