// DirDataReader.h

#ifndef _DIR_DATA_READER_H_
#define _DIR_DATA_READER_H_

/////////////
// includes

#include "FileIDToPathMap.h"
#include "G2TaskOutputReader.h"
#include "OpenDirCache.h"
#include "PathGroup.h"
#include "QueueProcessor.h"
#include "ReaderStats.h"
#include "ResponseTimeStats.h"
#include "SRFSConstants.h"
#include "SRFSDHT.h"
#include "Util.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>


////////////
// defines

#define DDR_NO_AUTO_CREATE	0
#define DDR_AUTO_CREATE	1


//////////
// types

typedef struct DirDataReader {
	OpenDirCache	*openDirCache;
	QueueProcessor	*dirDataQueueProcessor;
	SRFSDHT			*sd;
	SKSession		*(pSession[DDR_DHT_THREADS]);
    SKAsyncNSPerspective *(ansp[DDR_DHT_THREADS]);
	ResponseTimeStats	*rtsDirData;
    SKGetOptions	*metaDataGetOptions;
    SKGetOptions	*valueAndMetaDataGetOptions;    
} DirDataReader;


///////////////
// prototypes

DirDataReader *ddr_new(SRFSDHT *sd, ResponseTimeStats *rtsDirData, OpenDirCache *openDirCache);
void ddr_delete(DirDataReader **ddr);
ActiveOp *ddr_create_active_op(void *_ddr, void *_path, uint64_t noMinModificationTime);
int ddr_get_OpenDir(DirDataReader *ddr, char *path, OpenDir **od, int createIfNotFound);
DirData *ddr_get_DirData(DirDataReader *ddr, char *path);
void ddr_check_for_update(DirDataReader *ddr, OpenDir *od);
void ddr_check_for_reconciliation(DirDataReader *ddr, char *path);
void ddr_update_OpenDir(DirDataReader *ddr, OpenDir	*od);

#endif
