// OpenDirWriter.h

#ifndef _OPEN_DIR_WRITER_H_
#define _OPEN_DIR_WRITER_H_

/////////////
// includes

#include "DirDataReader.h"
#include "OpenDir.h"
#include "NSKeySplit.h"
#include "QueueProcessor.h"
#include "SRFSConstants.h"
#include "SRFSDHT.h"
#include "Util.h"


//////////
// types

typedef struct OpenDirWriter {
	QueueProcessor	*qp;
	SRFSDHT			*sd;
	SKSession		*pSession;
    SKAsyncNSPerspective	*ansp;
	// retry logic is currently deprecated in favor of periodic reconciliation
	//QueueProcessor	*retryQP;
	//DirDataReader	*ddr;
    uint64_t        minWriteIntervalMillis;
} OpenDirWriter;


///////////////
// prototypes

OpenDirWriter *odw_new(SRFSDHT *sd/*, DirDataReader *ddr*/, uint64_t minWriteIntervalMillis);
void odw_delete(OpenDirWriter **odw);
void odw_write_dir(OpenDirWriter *odw, const char *path, OpenDir *od);

#endif
