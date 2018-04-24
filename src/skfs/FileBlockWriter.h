// FileBlockWriter.h

#ifndef _FILE_BLOCK_WRITER_H_
#define _FILE_BLOCK_WRITER_H_

/////////////
// includes

#include "ActiveOpRef.h"
#include "FileBlockID.h"
#include "FileBlockCache.h"
#include "QueueProcessor.h"
#include "SRFSConstants.h"
#include "SRFSDHT.h"
#include "Util.h"
#include "WritableFileBlock.h"

#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>


//////////
// types

typedef struct FileBlockWriter {
	QueueProcessor	*qp;
	SRFSDHT			*sd;
	int				useCompression;
	SKSession		*pSession;
    SKAsyncNSPerspective *ansp;
	SKSession		*_pSession[FBW_DHT_SESSIONS];
    SKAsyncNSPerspective *_ansp[FBW_DHT_SESSIONS];
    FileBlockCache  *fbc;
} FileBlockWriter;

typedef struct FBW_ActiveDirectPut {
	char		key[SRFS_FBID_KEY_SIZE];
	SKVal		*pVal;
	SKAsyncPut	*pPut;
} FBW_ActiveDirectPut;


///////////////
// prototypes

FileBlockWriter *fbw_new(SRFSDHT *sd, int useCompression, FileBlockCache *fbc, int reliableQueue = FALSE);
void fbw_delete(FileBlockWriter **fbw);
void fbw_write_file_block(FileBlockWriter *fbw, FileBlockID *fbid, size_t dataLength, void *data, ActiveOpRef *aor);
FBW_ActiveDirectPut *fbw_put_direct(FileBlockWriter *fbw, FileBlockID *fbid, WritableFileBlock *wfb);
SKOperationState::SKOperationState fbw_wait_for_direct_put(FileBlockWriter *fbw, FBW_ActiveDirectPut **_adp);
void fbw_invalidate_file_blocks(FileBlockWriter *fbw, FileID *fid, int numRequests);

#endif
