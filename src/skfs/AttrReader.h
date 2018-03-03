// AttrReader.h

#ifndef _ATTR_READER_H_
#define _ATTR_READER_H_

/////////////
// includes

#include "AttrCache.h"
#include "AttrWriter.h"
#include "Cache.h"
#include "FileAttr.h"
#include "FileIDToPathMap.h"
#include "G2TaskOutputReader.h"
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

struct OpenDirTable;


//////////
// types

typedef struct AttrReader {
	AttrCache		*attrCache;
    uint64_t        attrTimeoutMillis;
	QueueProcessor	*nfsAttrQueueProcessor;
	QueueProcessor	*dhtAttrQueueProcessor;
	QueueProcessor	*attrPrefetchProcessor;
	char nfsRoots[SRFS_MAX_NFS_ALIASES][SRFS_MAX_PATH_LENGTH];
	char nfsLocalAliases[SRFS_MAX_NFS_ALIASES][SRFS_MAX_PATH_LENGTH];
	char externalDirs[SRFS_MAX_EXTERNAL_DIRS][SRFS_MAX_PATH_LENGTH];
	int numExternalDirs;
	int numNFSAliases;
	PathGroup	*noErrorCachePaths;
	PathGroup	*noLinkCachePaths;
	PathGroup	*snapshotOnlyPaths;
	PathGroup	*taskOutputPaths;
	FileIDToPathMap *f2p;
	SRFSDHT		*sd;
	SKSession		*(pSession[FBR_DHT_THREADS]);
    SKAsyncNSPerspective *(ansp[FBR_DHT_THREADS]);
	AttrWriter	*aw;
	ResponseTimeStats	*rtsDHT;
	ResponseTimeStats	*rtsNFS;
	ReaderStats	*rs;
	G2TaskOutputReader	*g2tor;
} AttrReader;


///////////////
// prototypes

AttrReader *ar_new(FileIDToPathMap *f2p, SRFSDHT *sd, AttrWriter *aw, 
				   ResponseTimeStats *dhtStats, ResponseTimeStats *nfsStats, int numSubCaches,
                   uint64_t attrTimeoutMillis);
void ar_delete(AttrReader **ar);
void ar_set_g2tor(AttrReader *ar, G2TaskOutputReader *g2tor);
void ar_parse_no_error_cache_paths(AttrReader *ar, char *paths);
void ar_parse_no_link_cache_paths(AttrReader *ar, char *paths);
void ar_parse_snapshot_only_paths(AttrReader *ar, char *paths);
void ar_parse_native_aliases(AttrReader *ar, char *nfsMapping);
void ar_create_alias_dirs(AttrReader *ar, OpenDirTable *odt);
void ar_store_dir_attribs(AttrReader *ar, char *path, uint16_t mode = 0755);
int ar_get_attr_stat(AttrReader *ar, char *path, struct stat *st);
int ar_get_attr(AttrReader *ar, char *path, FileAttr *fa, uint64_t minModificationTimeMicros = 0);
void ar_prefetch(AttrReader *ar, char *parent, char *child);
CacheStoreResult ar_store_attr_in_cache_static(char *path, FileAttr *fa, int replace, uint64_t modificationTimeMicros, uint64_t timeoutMillis);
ActiveOp *ar_create_active_op(void *_ar, void *_nfsPath, uint64_t minModificationTimeMicros);
int ar_is_no_link_cache_path(AttrReader *ar, char *path);
AttrCache *ar_get_attrCache(AttrReader *ar);
void ar_ensure_path_fid_associated(AttrReader *ar, char * path, FileID *fid);

void ar_translate_path(AttrReader *ar, char *nfsPath, const char *path);
void ar_display_stats(AttrReader *ar, int detailedStats);

#endif
