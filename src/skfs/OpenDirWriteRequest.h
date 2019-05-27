// OpenDirWriteRequest.h

#ifndef _OPEN_DIR_WRITE_REQUEST_H_
#define _OPEN_DIR_WRITE_REQUEST_H_

/////////////
// includes

/////////////
// includes

#include "OpenDirWriter.h"
#include "OpenDir.h"
#include "NSKeySplit.h"
#include "Util.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>


//////////
// types

typedef struct OpenDirWriteRequest {
    OpenDirWriter    *openDirWriter;
    OpenDir            *od;
} OpenDirWriteRequest;


///////////////
// prototypes

OpenDirWriteRequest *odwr_new(OpenDirWriter *openDirWriter, OpenDir *od);
void odwr_delete(OpenDirWriteRequest **odwr);

#endif
