// FileBlockWriteRequest.h

#ifndef _FILE_BLOCK_WRITE_REQUEST_H_
#define _FILE_BLOCK_WRITE_REQUEST_H_

/////////////
// includes

/////////////
// includes

#include "ActiveOpRef.h"
#include "FileBlockWriter.h"
#include "NSKeySplit.h"
#include "Util.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>


//////////
// types

typedef struct FileBlockWriteRequest {
	FileBlockWriter	*fileBlockWriter;
	FileBlockID		*fbid;
	size_t			dataLength;
	void			*data;
	ActiveOpRef		*aor;
} FileBlockWriteRequest;


///////////////
// prototypes

FileBlockWriteRequest *fbwr_new(FileBlockWriter *fileBlockWriter, FileBlockID *fbid,
								 size_t dataLength, void *data, ActiveOpRef *aor);
void fbwr_delete(FileBlockWriteRequest **fbwr);

#endif
