// DirEntry.h

#ifndef _DIR_ENTRY_H_
#define _DIR_ENTRY_H_

/////////////
// includes

#include <fuse.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "FileStatus.h"
#include "OpenDirUpdate.h"
#include "SRFSConstants.h"
#include "SRFSDHT.h"
#include "Util.h"


////////////
// defines

#define DE_MAGIC    0xadab
#define DE_HEADER_BYTES    ((uint64_t)(&((DirEntry *)0)->data))
#define DE_MAX_NAME_LENGTH ((uint16_t)0xffffu)
#define DE_F_DELETED    0x1


//////////
// types

/**
 * Single directory entry in a DirData structure.
 */
typedef struct DirEntry {
    uint16_t    magic;
    uint16_t    dataSize;
    FileStatus    status;
    uint64_t    version;
    char        data[]; // must be last in structure for DE_HEADER_BYTES calculation
} DirEntry;


//////////////////////
// public prototypes

int de_compute_total_size_from_name_length(int length);
int de_compute_total_size_from_name(char *s);
int de_compute_data_size_from_name_length(int length);
int de_compute_data_size_from_name(char *s);
DirEntry *de_init(DirEntry *de, int dataSize, FileStatus status, uint64_t version, char *data);
DirEntry *de_init_from_de(DirEntry *de, DirEntry *oDE);
DirEntry *de_init_from_update(DirEntry *de, OpenDirUpdate *update);
DirEntry *de_initial(const char *blob, DirEntry *limit);
DirEntry *de_next(DirEntry *de, DirEntry *limit, int sanityCheck = TRUE);
int de_sanity_check(DirEntry *de, int fatalErrorOnFailure = TRUE);
void de_set_deleted(DirEntry *de, int deleted, uint64_t version);
int de_is_deleted(DirEntry *de);
const char *de_get_name(DirEntry *de);
void de_display(DirEntry *de, FILE *file = stdout);
void de_update(DirEntry *de, OpenDirUpdate *odu);

#endif /* _DIR_ENTRY_H_ */
