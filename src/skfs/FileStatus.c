// FileStatus.c

/////////////
// includes

#include "FileStatus.h"


////////////
// defines

#define FS_F_DELETED 1
#define FS_S_DELETED "deleted"
#define FS_S_NOT_DELETED ""


///////////////////
// implementation

int fs_get_deleted(FileStatus *fs) {
    return *fs & FS_F_DELETED;
}

void fs_set_deleted(FileStatus *fs, int deleted) {
    if (deleted) {
        *fs = *fs | FS_F_DELETED;
    } else {
        *fs = *fs & (~FS_F_DELETED);
    }
}

const char *fs_to_string(FileStatus *fs) {
    if (fs_get_deleted(fs)) {
        return FS_S_DELETED;
    } else {
        return FS_S_NOT_DELETED;
    }
}
