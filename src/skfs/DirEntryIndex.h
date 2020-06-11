// DirEntryIndex.h

#ifndef _DIR_ENTRY_INDEX_H_
#define _DIR_ENTRY_INDEX_H_

/////////////
// includes

#include "DirData.h"
#include "DirEntry.h"
#include "Util.h"

#include <stdint.h>


////////////
// defines

#define DEI_MAGIC    0xaaddaabb
#define DEI_HEADER_BYTES    ((uint64_t)(&((DirEntryIndex *)0)->entries))
#define DEI_ENTRY_SIZE sizeof(DEIndexEntry)
#define DEI_NOT_FOUND 0xffffffff

//////////
// types

typedef uint32_t DEIndexEntry;

/**
 * Single directory entry in a DirData structure.
 */
typedef struct DirEntryIndex {
    uint32_t        magic;
    uint32_t        numEntries;
    DEIndexEntry    entries[]; // must be last in structure for DE_HEADER_BYTES calculation
} DirEntryIndex;


//////////////////////
// public prototypes

void dei_init(DirEntryIndex *dei, uint32_t numEntries);
int dei_sanity_check(DirEntryIndex *dei, int fatalErrorOnFailure = TRUE);
DEIndexEntry *deie_set(DEIndexEntry *indexEntry, DirData *dd, DirEntry *de);
DirEntry *dei_get_dir_entry(DirEntryIndex *dei, DirData *dd, uint32_t index, int fatalErrorOnFailure = TRUE);
uint32_t dei_locate(DirEntryIndex *dei, DirData *dd, const char *name);
void dei_reindex(DirEntryIndex *dei, DirData *dd);
void dei_add_numEntries_and_reindex(DirEntryIndex *dei, DirData *dd, uint32_t numNewEntries);
/*
 create from dirdata
 but this requires shifting all entries when we add data...
 maybe make this a balanced binary tree? splay? cuckoo?
 don't bother? all of these would also require the same issue...
*/

#endif /* _DIR_ENTRY_INDEX_H_ */
