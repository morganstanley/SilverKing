//SKFSOpenFile.h

#ifndef _SKFS_OPEN_FILE_H_
#define _SKFS_OPEN_FILE_H_

/////////////
// includes

#include "FileAttr.h"
#include "WritableFileReference.h"

#include <stdint.h>


//////////
// types

typedef enum SKFSOpenFileType {OFT_WritableFile_Write, OFT_WritableFile_Read, OFT_NativeRelay, OFT_NativeStandard} SKFSOpenFileType;

typedef struct SKFSOpenFile {
    uint16_t            magic;
    SKFSOpenFileType    type;
    WritableFileReference     *wf_ref;
    int                 fd;
    FileAttr            *attr;
    char                *nativePath;
    off_t               nextPrereadBlock;
} SKFSOpenFile;


//////////////////////
// public prototypes

SKFSOpenFile *sof_new();
void sof_delete(SKFSOpenFile **sof);
int sof_is_valid(SKFSOpenFile *sof);

#endif