// FileBlockID.h

#ifndef _FILE_BLOCK_ID_
#define _FILE_BLOCK_ID_

/////////////
// includes

#include "FileID.h"

#include <stdint.h>


//////////
// types

typedef struct FileBlockID {
    FileID        fid;
    uint64_t    block;
    unsigned int   hash;
} FileBlockID;


//////////////////////
// public prototypes

FileBlockID *fbid_new(FileID *fid, uint64_t block);
FileBlockID *fbid_new_native(struct stat *_stat, uint64_t block);
FileBlockID *fbid_dup(FileBlockID *fbid);
void fbid_delete(FileBlockID **fbid);
unsigned int fbid_hash(FileBlockID *fbid);
int fbid_compare(FileBlockID *a, FileBlockID *b);
FileID *fbid_get_id(FileBlockID *fbid);
uint64_t fbid_get_block(FileBlockID *fbid);
size_t fbid_block_size(FileBlockID *fbid);
off_t fbid_block_offset(FileBlockID *fbid);
int fbid_is_last_block(FileBlockID *fbid);
int fbid_to_string(FileBlockID *fbid, char *dest);

#endif
