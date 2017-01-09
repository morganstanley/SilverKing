// PartialBlockReadRequest.h

#ifndef _PARTIAL_BLOCK_READ_REQUEST_H_
#define _PARTIAL_BLOCK_READ_REQUEST_H_

/////////////
// includes

#include "FileBlockID.h"
#include "Util.h"

#include <stddef.h>


//////////
// types

typedef struct PartialBlockReadRequest {
	FileBlockID	*fbid;
	void		*dest;
	size_t		readOffset;
	size_t		readSize;
    uint64_t    minModificationTimeMillis;
} PartialBlockReadRequest;


///////////////
// prototypes

PartialBlockReadRequest *pbrr_new(FileBlockID *fbid, void *dest, size_t readOffset, size_t readSize, uint64_t minModificationTimeMillis);
void pbrr_delete(PartialBlockReadRequest **pbrr);

#endif
