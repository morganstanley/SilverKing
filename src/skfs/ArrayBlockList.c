// ArrayBlockList.c

/////////////
// includes

#include <stdlib.h>

#include "ArrayBlockList.h"
#include "Util.h"


///////////////////////
// private prototypes

static ArrayBlock *ab_new(size_t initialBlockSize);
static void ab_delete(ArrayBlock **ab);
static ArrayBlock *ab_add(ArrayBlock *ab, void *entry, size_t maxBlockSize, size_t blockIncrement);


//////////////////////////////
// ArrayBlock implementation

static ArrayBlock *ab_new(size_t initialBlockSize) {
    ArrayBlock    *ab;

    ab = (ArrayBlock *)mem_alloc(1, sizeof(ArrayBlock));
    ab->curBlockSize = initialBlockSize;
    ab->entries = (void **)mem_alloc(initialBlockSize, sizeof(void *));
    return ab;
}

/**
 * Delete an AB instance.
 */
static void ab_delete(ArrayBlock **ab, int freeEntries) {
    if (ab != NULL && *ab != NULL) {
        if (freeEntries) {
            size_t    i;
            
            for (i = 0; i < (*ab)->numEntries; i++) {
                if ((*ab)->entries[i] != NULL) {
                    mem_free(&(*ab)->entries[i]);
                }
            }
        }
        mem_free((void **) &((*ab)->entries) );
        mem_free((void **)ab);
    } else {
        fatalError("bad ptr passed to ab_delete");
    }
}

static ArrayBlock *ab_add(ArrayBlock *ab, void *entry, size_t maxBlockSize, size_t blockIncrement) {
    //srfsLog(LOG_FINE, "ab_add %llx %llx %u %d", ab, entry, maxBlockSize, blockIncrement);
    if (ab->numEntries >= ab->curBlockSize) {
        //srfsLog(LOG_FINE, "ab->numEntries >= ab->curBlockSize. %d %d", ab->numEntries, ab->curBlockSize);
        if (ab->curBlockSize >= maxBlockSize) {
        //if (ab->curBlockSize + blockIncrement > maxBlockSize) {
            //srfsLog(LOG_FINE, "ab->curBlockSize + blockIncrement > maxBlockSize");
            return NULL;
        } else {
            size_t    newBlockSize;
            
            newBlockSize = size_min(ab->curBlockSize + blockIncrement, maxBlockSize);
            //srfsLog(LOG_FINE, "mem_realloc");
            mem_realloc((void **)&ab->entries, ab->curBlockSize, newBlockSize, sizeof(void *));
            ab->curBlockSize = newBlockSize;
        }
    }
    ab->entries[ab->numEntries] = entry;
    ab->numEntries++;
    return ab;
}

static void *ab_get(ArrayBlock *ab, size_t index) {
    if (index >= ab->curBlockSize) {
        fatalError("index >= ab->curBlockSize", __FILE__, __LINE__);
    }
    return ab->entries[index];
}

static void ab_free_entry(ArrayBlock *ab, size_t index) {
    if (index >= ab->curBlockSize) {
        fatalError("index >= ab->curBlockSize", __FILE__, __LINE__);
    }
    if (ab->entries[index] != NULL) {
        mem_free(&ab->entries[index]);
    } else {
        srfsLog(LOG_FINE, "ab_free_entry ignoring already free entry");
    }
}

static void ab_truncate_block(ArrayBlock *ab, size_t newNumEntries) {
    if (newNumEntries > ab->numEntries) {
        fatalError("newNumEntries > ab->numEntries", __FILE__, __LINE__);
    }
    for (size_t i = newNumEntries; i < ab->numEntries; i++) {
        ab_free_entry(ab, i);
    }
    ab->numEntries = newNumEntries;
}

//////////////////////////////////
// ArrayBlockList implementation

/**
 * Create a new ABL instance.
 */
ArrayBlockList *abl_new(size_t initialBlockSize, size_t maxBlockSize, size_t blockIncrement, int freeEntriesOnDelete) {
    ArrayBlockList    *abl;

    srfsLog(LOG_FINE, "abl_new %u %u %u %s", initialBlockSize, maxBlockSize, blockIncrement, freeEntriesOnDelete ? "TRUE" : "FALSE");
    abl = (ArrayBlockList *)mem_alloc(1, sizeof(ArrayBlockList));
    abl->freeEntriesOnDelete = freeEntriesOnDelete;
    abl->initialBlockSize = initialBlockSize;
    abl->maxBlockSize = maxBlockSize;
    abl->blockIncrement = blockIncrement;
    abl->blocks = (ArrayBlock **)mem_alloc(1, sizeof(ArrayBlock *));
    abl->blocks[0] = ab_new(initialBlockSize);
    abl->numBlocks = 1;
    srfsLog(LOG_FINE, "abl_new exit abl %llx abl->blocks[0]->curBlockSize %u abl->numBlocks %u", abl, abl->blocks[0]->curBlockSize, abl->numBlocks);
    return abl;
}

/**
 * Delete an ABL instance.
 */
void abl_delete(ArrayBlockList **abl) {
    if (abl != NULL && *abl != NULL) {
        size_t    i;
        
        for (i = 0; i < (*abl)->numBlocks; i++) {
            ab_delete(&((*abl)->blocks[i]), (*abl)->freeEntriesOnDelete);
        }
        mem_free((void **) &((*abl)->blocks) );
        mem_free((void **)abl);
    } else {
        fatalError("bad ptr passed to abl_delete");
    }
}

static size_t abl_cur_block_index(ArrayBlockList *abl) {
    if (abl->size == 0) {
        return 0;
    } else {
        return (abl->size - 1) / abl->maxBlockSize;
    }
}

static void abl_add_block(ArrayBlockList *abl) {
    //srfsLog(LOG_FINE, "abl_add_block");
    mem_realloc((void **)&abl->blocks, abl->numBlocks, abl->numBlocks + 1, sizeof(ArrayBlock *));
    abl->blocks[abl->numBlocks] = ab_new(abl->initialBlockSize);
    abl->numBlocks++;
}

size_t abl_add(ArrayBlockList *abl, void *entry) {
    ArrayBlock    *ab;
    size_t      curBlockIndex;
    
    curBlockIndex = abl_cur_block_index(abl);
    srfsLog(LOG_FINE, "abl_add %llx %u", abl, abl->numBlocks);
    ab = ab_add(abl->blocks[curBlockIndex], entry, abl->maxBlockSize, abl->blockIncrement);
    if (ab == NULL) {
        abl_add_block(abl);
        ab = ab_add(abl->blocks[curBlockIndex + 1], entry, abl->maxBlockSize, abl->blockIncrement);
        if (ab == NULL) {
            fatalError("Couldn't add entry after adding new block", __FILE__, __LINE__);
        }
    }
    abl->size++;
    return abl->size;
}

void *abl_get(ArrayBlockList *abl, size_t index) {
    size_t    blockIndex;
    size_t    blockOffset;
    
    blockIndex = index / abl->maxBlockSize;
    blockOffset = index % abl->maxBlockSize;
    if (blockIndex >= abl->numBlocks) {
        fatalError("blockIndex >= abl->numBlocks", __FILE__, __LINE__);
    }
    return ab_get(abl->blocks[blockIndex], blockOffset);
}

static void abl_free_entry(ArrayBlockList *abl, size_t index) {
    size_t    blockIndex;
    size_t    blockOffset;
    
    blockIndex = index / abl->maxBlockSize;
    blockOffset = index % abl->maxBlockSize;
    if (blockIndex >= abl->numBlocks) {
        fatalError("blockIndex >= abl->numBlocks", __FILE__, __LINE__);
    }
    ab_free_entry(abl->blocks[blockIndex], blockOffset);
}

size_t abl_size(ArrayBlockList *abl) {
    return abl->size;
}

void abl_truncate(ArrayBlockList *abl, size_t newSize) {
    if (newSize > abl->size) {
        fatalError("newSize > abl->size", __FILE__, __LINE__);
    } else if (newSize < abl->size) {
        size_t    newEndBlockIndex;
        size_t    oldEndBlockIndex;
        size_t    newEndBlockSize;

        if (newSize > 0) {
            newEndBlockIndex = (newSize - 1) / abl->maxBlockSize;
        } else {
            newEndBlockIndex = 0;
        }
        oldEndBlockIndex = (abl->size - 1) / abl->maxBlockSize;
        newEndBlockSize = newSize % abl->maxBlockSize;
        
        for (size_t i = newEndBlockIndex + 1; i <= oldEndBlockIndex; i++) {
            ab_truncate_block(abl->blocks[i], 0);
        }
        ab_truncate_block(abl->blocks[newEndBlockIndex], newEndBlockSize);
        
        abl->size = newSize;
    } else {
        // size unchanged; no work to be done
    }
}
