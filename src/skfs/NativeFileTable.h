// NativeFileTable.h

#ifndef _NATIVE_FILE_TABLE_H_
#define _NATIVE_FILE_TABLE_H_

/////////////
// includes

#include "HashTableAndLock.h"
#include "Util.h"
#include "NativeFileReference.h"

#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>


////////////
// defines

#define NFT_NUM_HT	32
#define NFT_REF_TABLE_SIZE  1024


//////////
// types

typedef struct NativeFileTable {
    const char	*name;
	HashTableAndLock	htl[NFT_NUM_HT];
	pthread_spinlock_t	tableRefsLock;
    NativeFileReference *tableRefs[NFT_REF_TABLE_SIZE];
    int nextRefIndex;
} NativeFileTable;


//////////////////////
// public prototypes

NativeFileTable *nft_new(const char *name);
void nft_delete(NativeFileTable **nft);
NativeFileReference *nft_open(NativeFileTable *wft, const char *name);

#endif
