// WritableFileBlock.h

#ifndef _WRITABLE_FILE_BLOCK_H_
#define _WRITABLE_FILE_BLOCK_H_

/////////////
// includes

#include "SRFSConstants.h"

#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>


////////////
// defines



//////////
// types

typedef struct WritableFileBlock {
    unsigned char	block[SRFS_BLOCK_SIZE];
	size_t	size;
} WritableFileBlock;


//////////////////////
// public prototypes

WritableFileBlock *wfb_new();
void wfb_delete(WritableFileBlock **wfb);
size_t wfb_write(WritableFileBlock *wfb, const char *src, size_t length);
size_t wfb_rewrite(WritableFileBlock *wfb, const char *src, size_t offset, size_t length);
void wfb_truncate(WritableFileBlock *wfb, size_t length);
int wfb_is_full(WritableFileBlock *wfb);
int wfb_is_empty(WritableFileBlock *wfb);
size_t wfb_zero_out_remainder(WritableFileBlock *wfb);

#endif
