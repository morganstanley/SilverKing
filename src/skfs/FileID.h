// FileID.h

#ifndef _FILE_ID_H_
#define _FILE_ID_H_

/////////////
// includes

#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>


////////////
// defines

#define FID_MAX_STRING_SIZE 128
#define fid_is_native_fs(X) ((X)->fileSystem == fsNative)


//////////
// types

typedef enum FileSystem {fsNative, fsSKFS, fsSKFSInternal} FileSystem;

typedef struct NativeFileID {
    ino_t    inode;
    time_t    creationTime;
    time_t    modTime;
    off_t    size;
} NativeFileID;

typedef struct SKFSFileID {
    uint64_t    instance;
    uint64_t    sequence;
} SKFSFileID;

typedef struct FileID {
    FileSystem    fileSystem;
    unsigned int    hash;
    union {
        NativeFileID    native;
        SKFSFileID        skfs;
    };
} FileID;


///////////////
// prototypes

void fid_module_init(uint64_t instance);
void fid_init_native(FileID *fid, struct stat *_stat);
void fid_init_skfs(FileID *fid, uint64_t instance, uint64_t sequence);
FileID *fid_dup(FileID *fid);
FileID *fid_new_native(struct stat *_stat);
FileID *fid_new_skfs(uint64_t instance, uint64_t sequence);
FileID *fid_generate_new_skfs();
FileID *fid_generate_new_skfs_internal();
void fid_delete(FileID **fid);
off_t fid_get_size(FileID *fid);
unsigned int fid_hash(FileID *fid);
int fid_compare(FileID *a, FileID *b);
int fid_to_string(FileID *fid, char *dest);
ino_t fid_get_inode(FileID *fid);
void fid_generate_and_init_skfs(FileID *fid);
void fid_generate_and_init_skfs_internal(FileID *fid);

#endif
