// PartialBlockReadRequest.c

/////////////
// includes

#include "PartialBlockReadRequest.h"
#include "SRFSConstants.h"
#include "Util.h"


///////////////////
// implementation

PartialBlockReadRequest *pbrr_new(FileBlockID *fbid, void *dest, size_t readOffset, size_t readSize, uint64_t minModificationTimeMicros) {
	PartialBlockReadRequest *pbrr;

	srfsLog(LOG_FINE, "pbrr_new %llx %llx %lu %lu", fbid, dest, readOffset, readSize);
	if (readSize > SRFS_BLOCK_SIZE) {
		fatalError("readSize > SRFS_BLOCK_SIZE");
	}
	if (readOffset >= SRFS_BLOCK_SIZE) {
		fatalError("readOffset >= SRFS_BLOCK_SIZE");
	}
	pbrr = (PartialBlockReadRequest*)mem_alloc(1, sizeof(PartialBlockReadRequest));
	pbrr->fbid = fbid;
	pbrr->dest = dest;
	pbrr->readOffset = readOffset;
	pbrr->readSize = readSize;
    pbrr->minModificationTimeMicros = minModificationTimeMicros;
	return pbrr;
}

void pbrr_delete(PartialBlockReadRequest **pbrr) {
	if (pbrr != NULL && *pbrr != NULL) {
		mem_free((void **)pbrr);
	} else {
		fatalError("bad ptr in pbrr_delete");
	}
}
