// DirData.h

#ifndef _DIR_DATA_H_
#define _DIR_DATA_H_

/////////////
// includes

#include "DirEntry.h"
#include "OpenDirUpdate.h"
#include "SRFSConstants.h"
#include "SRFSDHT.h"
#include "Util.h"

#include <fuse.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdio.h>
#include <unistd.h>


////////////
// defines

#define DD_MAGIC	0x00abacad
#define DD_MAGIC_BYTES	4
#define DD_MAX_SIZE = (1024 * 1024 * 128)
#define DD_HEADER_BYTES	((uint64_t)(&((DirData *)0)->data))


//////////
// types

/**
 * An immutable structure that contains directory entries.
 * Copy-on-write is used in place of mutation.
 *
 * (The current structure is an inefficient proof-of-concept structure.
 * This will likely be replaced in the future after the concept
 * has been validated.)
 */
typedef struct DirData {
	uint32_t	magic;
	uint32_t	dataLength; // length of data[] in bytes, including DirEntries and the DirEntryIndex
	uint32_t	indexOffset; // offset from data[] to start of index in bytes
	uint32_t	numEntries;
	const char	data[]; // Must be last in structure for DD_HEADER_BYTES calculation
		/*
		Structure inside of data:
            DirEntry...
            DirEntryIndex
		*/
} DirData;

typedef struct MergeResult {
		DirData	*dd;
		int		dd0NotIn1;
		int		dd1NotIn0;
} MergeResult;


//////////////////////
// public prototypes

DirData *dd_new_empty();
DirData *dd_new(SKVal *skVal);
void dd_delete(DirData **dd);
int dd_sanity_check(DirData *dd, int fatalErrorOnFailure = TRUE);
int dd_sanity_check_full(DirData *dd, int fatalErrorOnFailure = TRUE);
size_t dd_length_with_header_and_index(DirData *dd);
DirData *dd_fuse_fi_fh_to_dd(struct fuse_file_info *fi);
DirData *dd_add_entries(DirData *dd, char **names, int numEntries);
DirData *dd_process_updates(DirData *dd, OpenDirUpdate *update, int numUpdates);
DirData *dd_dup(DirData *dd);
MergeResult dd_merge(DirData *dd1, DirData *dd2);
void dd_display(DirData *dd, FILE *file = stdout);
DirEntry *dd_get_entry(DirData *dd, uint32_t index);
int dd_is_empty(DirData *dd);

#endif /* _DIR_DATA_H_ */
