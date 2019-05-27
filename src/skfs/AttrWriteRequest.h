// AttrWriteRequest.h

#ifndef _ATTR_WRITE_REQUEST_H_
#define _ATTR_WRITE_REQUEST_H_

/////////////
// includes

/////////////
// includes

#include "AttrWriter.h"
#include "FileAttr.h"
#include "NSKeySplit.h"
#include "Util.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>


//////////
// types

typedef struct AttrWriteRequest {
    AttrWriter    *attrWriter;
    char        path[SRFS_MAX_PATH_LENGTH];
    FileAttr    fa;
} AttrWriteRequest;


///////////////
// prototypes

AttrWriteRequest *awr_new(AttrWriter *attrWriter, const char *path, FileAttr *fa);
void awr_delete(AttrWriteRequest **awr);

#endif
