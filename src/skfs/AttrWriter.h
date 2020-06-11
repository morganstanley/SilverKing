// AttrWriter.h

#ifndef _ATTR_WRITER_H_
#define _ATTR_WRITER_H_

/////////////
// includes

#include "AttrCache.h"
#include "FileAttr.h"
#include "NSKeySplit.h"
#include "QueueProcessor.h"
#include "SRFSConstants.h"
#include "SRFSDHT.h"
#include "Util.h"


////////////
// defines

// FUTURE - eliminate need for below
#define AW_NO_REQUIRED_PREV_VERSION -100


//////////
// types

typedef struct AttrWriter {
    QueueProcessor    *qp;
    SRFSDHT            *sd;
    SKSession        *pSession;
    SKAsyncNSPerspective    *ansp;
    SKPutOptions    *defaultPutOptions;
    SKInvalidationOptions *defaultInvalidationOptions;
} AttrWriter;

typedef struct AWWriteResult {
    SKOperationState::SKOperationState  operationState;
    int64_t storedVersion;
} AWWriteResult;


///////////////
// prototypes

AttrWriter *aw_new(SRFSDHT *sd);
void aw_delete(AttrWriter **aw);
void aw_write_attr(AttrWriter *aw, const char *path, FileAttr *fa);
AWWriteResult aw_write_attr_direct(AttrWriter *aw, const char *path, FileAttr *fa, 
                                        AttrCache *ac = NULL, int maxAttempts = 1, SKFailureCause::SKFailureCause *cause = NULL,
                                        int64_t requiredPreviousVersion = AW_NO_REQUIRED_PREV_VERSION, int16_t lockSeconds = 0);

#endif
