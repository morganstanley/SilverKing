// FileBlockReader.h

#ifndef _FILE_BLOCK_READER_H_
#define _FILE_BLOCK_READER_H_

/////////////
// includes

#include "FileBlockCache.h"
#include "FileBlockWriter.h"
#include "FileIDToPathMap.h"
#include "NativeFileTable.h"
#include "PartialBlockReadRequest.h"
#include "PathGroup.h"
#include "QueueProcessor.h"
#include "ReaderStats.h"
#include "ResponseTimeStats.h"
#include "SRFSConstants.h"
#include "SRFSDHT.h"
#include "Util.h"

#include <stdint.h>
#include <unistd.h>
#include <pthread.h>


//////////
// types

typedef struct FileBlockReader {
	FileIDToPathMap *f2p;
	FileBlockCache	*fileBlockCache;
	QueueProcessor	*dhtFileBlockQueueProcessor;
	QueueProcessor	*nfsFileBlockQueueProcessor;
	FileBlockWriter	*fbwCompress;
	FileBlockWriter	*fbwRaw;
    NativeFileTable *nft;
	SRFSDHT			*sd;
	ResponseTimeStats *rtsDHT;
	ResponseTimeStats *rtsNFS;
	ReaderStats		*rs;
	PathGroup		*compressedPaths;
	PathGroup		*noFBWPaths;
							// stats
	uint64_t		directNFS;
	uint64_t		compressedNFS;
	SKSession		*(pSession[FBR_DHT_THREADS]);
    SKAsyncNSPerspective *(ansp[FBR_DHT_THREADS]);
	pthread_spinlock_t	statLock;
} FileBlockReader;


///////////////
// prototypes

FileBlockReader *fbr_new(FileIDToPathMap *f2p, 
						 FileBlockWriter *fbwCompress, FileBlockWriter *fbwRaw,
						 SRFSDHT *sd, 
						 ResponseTimeStats *rtsDHT, ResponseTimeStats *rtsNFS,
                         FileBlockCache *fbc);
void fbr_delete(FileBlockReader **fbr);
int fbr_read(FileBlockReader *fbr, PartialBlockReadRequest **pbrr, int numRequests,
			PartialBlockReadRequest **pbrrsReadAhead, int numRequestsReadAhead,
            int presumeBlocksInDHT, int useNFSReadAhead);
int fbr_read_test(FileBlockReader *fbr, FileBlockID *fbid, void *dest, size_t readOffset, size_t readSize);
ActiveOp *fbr_create_active_op(void *_fbr, void *_fbid, uint64_t minModificationTimeMicros);
void fbr_parse_permanent_suffixes(FileBlockReader *fbReader, char *permanentSuffixes);
void fbr_display_stats(FileBlockReader *fbr, int detailedStats);
void *fbr_read_block_compressed_test(void *fbrr, size_t *_blockSize, char *path);
void fbr_parse_compressed_paths(FileBlockReader *fbr, char *paths);
void fbr_parse_no_fbw_paths(FileBlockReader *fbr, char *paths);

#endif
