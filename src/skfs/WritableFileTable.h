// WritableFileTable.h

#ifndef _WRITABLE_FILE_TABLE_H_
#define _WRITABLE_FILE_TABLE_H_

/////////////
// includes

#include "AttrReader.h"
#include "AttrWriter.h"
#include "FileAttr.h"
#include "FileBlockWriter.h"
#include "HashTableAndLock.h"
#include "PartialBlockReader.h"
#include "Util.h"
#include "WritableFile.h"
#include "WritableFileReference.h"

#include <stdlib.h>
#include <unistd.h>


////////////
// defines

#define WFT_NUM_HT	256


//////////
// types

typedef struct WritableFileTable {
    const char	*name;
	AttrCache	*ac;
	AttrWriter	*aw;
    AttrReader  *ar;
    FileBlockWriter *fbw;
	HashTableAndLock	htl[WFT_NUM_HT];
} WritableFileTable;


//////////////////////
// public prototypes

WritableFileTable *wft_new(const char *name, AttrWriter *aw, AttrCache *ac, AttrReader *ar, FileBlockWriter *fbw);
void wft_delete(WritableFileTable **wft);
WritableFileReference *wft_create_new_file(WritableFileTable *wft, const char *name, mode_t mode, 
                                    FileAttr *fa = NULL, PartialBlockReader *pbr = NULL);
int wft_contains(WritableFileTable *wft, const char *name);
WritableFileReference *wft_get(WritableFileTable *wft, const char *name);
int wft_delete_file(WritableFileTable *wft, const char *name, int deleteBlocks = TRUE);

#endif
