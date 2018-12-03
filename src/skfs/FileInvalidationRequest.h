// FileInvalidationRequest.h

#ifndef _FILE_INVALIDATION_REQUEST_H_
#define _FILE_INVALIDATION_REQUEST_H_

/////////////
// includes

/////////////
// includes

#include "ActiveOpRef.h"
#include "FileBlockWriter.h"
#include "FileID.h"
#include "NSKeySplit.h"
#include "Util.h"


//////////
// types

typedef struct FileInvalidationRequest {
	FileBlockWriter	*fileBlockWriter;
	FileID		    *fid;
	int 			numBlocks;
} FileInvalidationRequest;


///////////////
// prototypes

FileInvalidationRequest *fir_new(FileBlockWriter *fileBlockWriter, FileID *fid,
								 int numBlocks);
void fir_delete(FileInvalidationRequest **fir);

#endif
