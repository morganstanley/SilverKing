// DirDataWriter.h

#ifndef _DIR_DATA_WRITER_H_
#define _DIR_DATA_WRITER_H_

/////////////
// includes

#include "OpenDirUpdate.h"
#include "NSKeySplit.h"
#include "QueueProcessor.h"
#include "SRFSConstants.h"
#include "SRFSDHT.h"
#include "Util.h"


//////////
// types

typedef struct DirDataWriter {
    QueueProcessor    *(qp[DDW_DHT_QUEUE_PROCESSORS]);
    SRFSDHT            *sd;
    SKSession        *pSession;
    SKAsyncNSPerspective    *ansp;
    // retry logic is currently deprecated in favor of periodic reconciliation
    //QueueProcessor    *retryQP;
} DirDataWriter;


///////////////
// prototypes

DirDataWriter *ddw_new(SRFSDHT *sd);
void ddw_delete(DirDataWriter **ddw);
void ddw_update_dir(DirDataWriter *ddw, char *dirName, uint32_t type, uint64_t version, char *entryName);

#endif
