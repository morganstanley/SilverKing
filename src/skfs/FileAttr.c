// FileAttr.c

/////////////
// includes

#include "FileAttr.h"
#include "FileID.h"
#include "Util.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>


//////////////////////
// globals

FileAttr    deletion_fa; // all zero


///////////////////
// implementation

FileAttr *fa_new(FileID *fid, struct stat *stat) {
    FileAttr    *fa;

    fa = (FileAttr *)mem_alloc(1, sizeof(FileAttr));
    memcpy(&fa->fid, fid, sizeof(FileID));
    memcpy(&fa->stat, stat, sizeof(struct stat));
    srfsLog(LOG_FINE, "fa_new %llx", fa);
    return fa;
}

FileAttr *fa_new_native(struct stat *_stat) {
    FileAttr    *fa;

    fa = (FileAttr *)mem_alloc(1, sizeof(FileAttr));
    fa_init_native(fa, _stat);
    srfsLog(LOG_FINE, "fa_new_native %llx", fa);
    return fa;
}

void fa_init_native(FileAttr *fa, struct stat *_stat) {
    fid_init_native(&fa->fid, _stat);
    memcpy(&fa->stat, _stat, sizeof(struct stat));
    if (fa->stat.st_size == 0) {
        fa->stat.st_blocks = 0;
    } else {
        fa->stat.st_blocks = ((fa->stat.st_size - 1) / SRFS_BLOCK_SIZE) + 1;
    }
    srfsLog(LOG_FINE, "fa_init_native %llx", fa);
}

void fa_delete(FileAttr **fa) {
    if (fa != NULL && *fa != NULL) {
        srfsLog(LOG_FINE, "fa_delete %llx", *fa);
        mem_free((void **)fa);
    } else {
        fatalError("bad ptr passed to fa_delete");
    }
}

FileAttr *fa_mark_deleted(FileAttr *fa) {
    FileAttr    *_fa;
    
    _fa = fa_dup(fa);
    _fa->stat.st_nlink = 0;
    return _fa;
}

int fa_is_deleted_file(FileAttr *fa) {
    return fa->stat.st_nlink == 0;
}

unsigned int fa_hash(FileAttr *fa) {
    return fid_hash(&fa->fid);
}

int fa_compare(FileAttr *a, FileAttr *b) {
    int    result;
    
    result = fid_compare(&a->fid, &b->fid);
    if (result == 0) {
        result = memcmp(a, b, sizeof(FileAttr));
    }
    return result;
}

int fa_to_string(FileAttr *fa, char *dest, size_t bufSize) {
    int    index;
    
    index = fid_to_string(&fa->fid, dest);
    if (index < 0) {
        fatalError("bad index", __FILE__, __LINE__);
    }
    if ((size_t)index < bufSize) {
        int    index2;
        
        index2 = sprintf(dest + index, "%lu %u %lu", fa->stat.st_ino, 
                                (unsigned int)(bufSize - index), fa->stat.st_nlink);
        if (index2 >= 0) {
            index += index2;
        } else {
            index = index;
        }
    }
    return index;
}

FileAttr *fa_dup(FileAttr *fa) {
    return (FileAttr *)mem_dup(fa, sizeof(FileAttr));
}

FileAttr *fa_get_deletion_fa() {
    return &deletion_fa;
}
