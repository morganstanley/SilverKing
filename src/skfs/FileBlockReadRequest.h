// FileBlockReadRequest.h

#ifndef _FILE_BLOCK_READ_REQUEST_H_
#define _FILE_BLOCK_READ_REQUEST_H_

/////////////
// includes

#include "FileBlockID.h"
#include "FileBlockReader.h"
#include "Util.h"


//////////
// types

typedef struct FileBlockReadRequest {
	FileBlockReader	*fileBlockReader;
	FileBlockID	*fbid;
    uint64_t    minModificationTimeMicros;
} FileBlockReadRequest;


///////////////
// prototypes

FileBlockReadRequest *fbrr_new(FileBlockReader *fileBlockReader, FileBlockID *fbid, 
                               uint64_t minModificationTimeMicros);
void fbrr_delete(FileBlockReadRequest **fbrr);
void fbrr_display(FileBlockReadRequest *fbrr, LogLevel level);

#endif
