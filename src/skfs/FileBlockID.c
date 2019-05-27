// FileBlockID.c

/////////////
// includes

#include "FileBlockID.h"
#include "SRFSConstants.h"
#include "Util.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

////////////////////
// private defines

#define FBID_ID_SIZE sizeof(FileID)


////////////////////
// private members

static int fbid_id_size = FBID_ID_SIZE;


///////////////////////
// private prototypes

static unsigned int _fbid_hash(FileBlockID *fbid);


///////////////////
// implementation

FileBlockID *fbid_new(FileID *fid, uint64_t block) {
    FileBlockID    *fbid;

    fbid = (FileBlockID *)mem_alloc(1, sizeof(FileBlockID));
    fbid->fid = *fid;
    fbid->block = block;
    fbid->hash = _fbid_hash(fbid);
    return fbid;
}

FileBlockID *fbid_new_native(struct stat *_stat, uint64_t block) {
    FileBlockID    *fbid;

    fbid = (FileBlockID *)mem_alloc(1, sizeof(FileBlockID));
    fid_init_native(&fbid->fid, _stat);
    fbid->block = block;
    fbid->hash = _fbid_hash(fbid);
    return fbid;
}

FileBlockID *fbid_dup(FileBlockID *_fbid) {
    FileBlockID    *fbid;

    fbid = (FileBlockID *)mem_alloc(1, sizeof(FileBlockID));
    memcpy(fbid, _fbid, sizeof(FileBlockID));
    return fbid;
}

void fbid_delete(FileBlockID **fbid) {
    if (fbid != NULL && *fbid != NULL) {
        mem_free((void **)fbid);
    } else {
        fatalError("bad ptr passed to fbid_delete");
    }
}

unsigned int fbid_hash(FileBlockID *fbid) {
    return fbid->hash;
}

// we split the id and block hash computation and equality checks 
// to allow for the possibility of changing the id in the future

static unsigned int _fbid_hash(FileBlockID *fbid) {
    unsigned char *str;
    unsigned long hash;
    int    i;
    int c;
   
    str = (unsigned char *)&fbid->fid;
    hash = 0;
    for (i = 0; i < fbid_id_size; i++) {
        c = *str;
        //fprintf(stdout, "%d\t%x\n", i, c);
        str++;
        hash = c + (hash << 6) + (hash << 16) - hash;
    }
    hash = hash ^ fbid->block;
    return hash;
}

int fbid_compare(FileBlockID *a, FileBlockID *b) {
    if (a->hash < b->hash) {
        return -1;
    } else if (a->hash > b->hash) {
        return 1;
    } else {
        int compareResult;
        
        compareResult = memcmp((void *)&a->fid, (void *)&b->fid, FBID_ID_SIZE);
        if (compareResult == 0) {
            if (a->block < b->block) {
                compareResult = -1;
            } else if (a->block > b->block) {
                compareResult = 1;
            }
        }
        return compareResult;
    }
}

FileID *fbid_get_id(FileBlockID *fbid) {
    return &fbid->fid;
}

uint64_t fbid_get_block(FileBlockID *fbid) {
    return fbid->block;
}

static uint64_t fbid_last_block(FileBlockID *fbid) {
    if ((uint64_t)fid_get_size(&fbid->fid) % SRFS_BLOCK_SIZE != 0) {
        return (uint64_t)fid_get_size(&fbid->fid) / SRFS_BLOCK_SIZE;
    } else {
        return (uint64_t)fid_get_size(&fbid->fid) / SRFS_BLOCK_SIZE - 1;
    }
}

size_t fbid_block_size(FileBlockID *fbid) {
    if (!fid_is_native_fs(&fbid->fid)) {
        fatalError("fbid_block_size called for non-native fs", __FILE__, __LINE__);
        return 0;
    } else {
        uint64_t    lastBlock;

        lastBlock = fbid_last_block(fbid);
        if (fbid->block < lastBlock) {
            return SRFS_BLOCK_SIZE;
        } else {
            size_t    blockSize;

            blockSize = fid_get_size(&fbid->fid) % SRFS_BLOCK_SIZE;
            if (blockSize == 0) {
                blockSize = SRFS_BLOCK_SIZE;
            }
            return blockSize;
            /*
            if (fbid->id.size != SRFS_BLOCK_SIZE) {
                return fbid->id.size % SRFS_BLOCK_SIZE;
            } else {
                return SRFS_BLOCK_SIZE;
            }
            */
        }
    }
}

off_t fbid_block_offset(FileBlockID *fbid) {
    return (off_t)(fbid->block * SRFS_BLOCK_SIZE);
}

int fbid_is_last_block(FileBlockID *fbid) {
    return fbid->block == fbid_last_block(fbid);
}

// consider accepting buffer size
// for now we go with the higher performance implementation
int fbid_to_string(FileBlockID *fbid, char *dest) {
    char    tmp[FID_MAX_STRING_SIZE];

    fid_to_string(&fbid->fid, tmp);
    return sprintf(dest, "%s:%lu", tmp, fbid->block);
}
