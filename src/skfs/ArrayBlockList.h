// ArrayBlockList.h

#ifndef _ARRAY_BLOCK_LIST_H_
#define _ARRAY_BLOCK_LIST_H_

/////////////
// includes

#include <stdlib.h>


//////////
// types

typedef struct ArrayBlock {
	size_t	curBlockSize;
	void	**entries;
	size_t	numEntries;
} ArrayBlock;

typedef struct ArrayBlockList {
	int 	freeEntriesOnDelete;
	size_t	initialBlockSize;
	size_t	maxBlockSize;
	size_t	blockIncrement;
	ArrayBlock	**blocks;
	size_t	numBlocks;
	size_t	size;
} ArrayBlockList;


//////////////////////
// public prototypes

ArrayBlockList *abl_new(size_t initialBlockSize, size_t maxBlockSize, size_t blockIncrement, int freeEntriesOnDelete);
void abl_delete(ArrayBlockList **abl);
size_t abl_add(ArrayBlockList *abl, void *data);
void *abl_get(ArrayBlockList *abl, size_t index);
size_t abl_size(ArrayBlockList *abl);
void abl_truncate(ArrayBlockList *abl, size_t newSize);

#endif
