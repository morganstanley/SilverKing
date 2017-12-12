// FileAttr.h

#ifndef _FILE_ATTR_H_
#define _FILE_ATTR_H_

/////////////
// includes

#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <FileID.h>


////////////
// defines

#define FA_NATIVE_LINK_MAGIC 127
#define FA_NATIVE_LINK_NORMAL 1

//////////
// types

typedef struct FileAttr {
	FileID	fid;
	struct stat	stat;
} FileAttr;

///////////////
// prototypes

FileAttr *fa_new(FileID *fid, struct stat *_stat);
FileAttr *fa_new_native(struct stat *_stat);
void fa_init_native(FileAttr *fa, struct stat *_stat);
void fa_delete(FileAttr **fa);
FileAttr *fa_mark_deleted(FileAttr *fa);
int fa_is_deleted_file(FileAttr *fa);
unsigned int fa_hash(FileAttr *fa);
int fa_compare(FileAttr *a, FileAttr *b);
int fa_to_string(FileAttr *fa, char *dest, size_t bufSize);
FileAttr *fa_dup(FileAttr *fa);
FileAttr *fa_get_deletion_fa();

#endif
