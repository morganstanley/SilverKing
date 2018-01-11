// WritableFile.h

#ifndef _WRITABLE_FILE_H_
#define _WRITABLE_FILE_H_

/////////////
// includes

#include "ArrayBlockList.h"
#include "AttrCache.h"
#include "AttrWriter.h"
#include "FileAttr.h"
#include "FileBlockCache.h"
#include "FileBlockWriter.h"
#include "HashTableAndLock.h"
#include "OpenDir.h"
#include "WritableFileReference.h"
#include "PartialBlockReader.h"
#include "WritableFileBlock.h"

#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>


////////////
// defines

#define WFR_MAX_REFS    128
#define WFR_RECYCLE_THRESHOLD    64
// WFR_RECYCLE_THRESHOLD must be < WFR_MAX_REFS for reference number recycling to be active


//////////
// types

typedef enum {WFR_Invalid = 0, WFR_Created, WFR_Destroyed} WFRefStatus;

typedef struct WritableFileReferentState {
	WFRefStatus	    refStatus[WFR_MAX_REFS];
	int				nextRef;
	int				toDelete;
} WritableFileReferentState;


#define _WF_TYPE_
typedef struct WritableFile {
	uint16_t	magic;	
    const char  *path;
    const char  *pendingRename;
    OpenDir     *parentDir;
    uint64_t    parentDirUpdateTimeMillis;
	FileAttr	fa;
	ArrayBlockList		*blockList;
    WritableFileBlock    *curBlock;
	uint64_t			numBlocks;
	pthread_mutex_t lock;
    HashTableAndLock    *htl;
	uint64_t	leastIncompleteBlockIndex;
    WritableFileReferentState   referentState;
    uint8_t kvAttrStale;
} WritableFile;


//////////////////////
// public prototypes

WritableFile *wf_new(const char *path, mode_t mode, HashTableAndLock *htl, AttrWriter *aw, 
                     FileAttr *fa = NULL, PartialBlockReader *pbr = NULL);
void wf_delete(WritableFile **wf);
void wf_set_parent_dir(WritableFile *, OpenDir *parentDir, uint64_t parentDirUpdateTimeMillis);
WritableFileReference *wf_add_reference(WritableFile *wf, char *file, int line);
void wf_set_pending_rename(WritableFile *wf, const char *newName);
int wf_write(WritableFile *wf, const char *src, size_t writeSize, off_t writeOffset, 
             FileBlockWriter *fbw, PartialBlockReader *pbr, FileBlockCache *fbc);
int wf_truncate(WritableFile *wf, off_t size, FileBlockWriter *fbw, PartialBlockReader *pbr);
int wf_flush(WritableFile *wf, AttrWriter *aw, FileBlockWriter *fbw, AttrCache *ac);
void wf_sanityCheckNumBlocks(WritableFile *wf, char *file, int line);
int wf_modify_attr(WritableFile *wf, mode_t *mode, uid_t *uid, gid_t *gid,
    const struct timespec *last_access_tp = NULL, const struct timespec *last_modification_tp = NULL, const struct timespec *last_change_tp = NULL);
WritableFile *wf_fuse_fi_fh_to_wf(struct fuse_file_info *fi);
int wf_create_ref(WritableFile *wf);
int wf_delete_ref(WritableFile *wf, int ref, AttrWriter *aw, FileBlockWriter *fbw, AttrCache *ac);
void wf_debug(WritableFile *wf);
void wf_sanity_check(WritableFile *wf);
void wf_set_sync_dir_updates(int syncDirUpdates);

#endif
