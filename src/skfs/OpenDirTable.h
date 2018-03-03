// OpenDirTable.h

#ifndef _OPEN_DIR_TABLE_H_
#define _OPEN_DIR_TABLE_H_

/////////////
// includes

#include "AttrReader.h"
#include "AttrWriter.h"
#include "DirData.h"
#include "DirDataReader.h"
#include "OpenDirCache.h"
#include "OpenDirWriter.h"

#include <pthread.h>
#include <sys/stat.h>


//////////
// types

typedef struct OpenDirTable {
	const char		*name;
	OpenDirCache	*odc;
	DirDataReader	*ddr;
	OpenDirWriter	*odw;
	AttrWriter		*aw;
	AttrReader		*ar;
	uint64_t		lastGetAttr;
	pthread_t		reconciliationThread;
    uint64_t        minReconciliationSleepMillis;
    uint64_t        maxReconciliationSleepMillis;
    uint64_t        _lastVersion;
    pthread_spinlock_t  lvLock;
} OpenDirTable;


//////////////////////
// public prototypes

OpenDirTable *odt_new(const char *name, SRFSDHT *sd, AttrWriter *aw, AttrReader *ar, ResponseTimeStats *rtsDirData, char *reconciliationSleep, uint64_t odwMinWriteIntervalMillis);
void odt_delete(OpenDirTable **odt);
int odt_opendir(OpenDirTable *odt, const char* path, struct fuse_file_info* fi);
int odt_readdir(OpenDirTable *odt, const char *path, void *buf, fuse_fill_dir_t filler,
                       off_t offset, struct fuse_file_info *fi);
int odt_releasedir(OpenDirTable *odt, const char* path, struct fuse_file_info *fi);
DirData *odt_get_DirData(OpenDirTable *odt, char *path);
int odt_rm_entry_from_parent_dir(OpenDirTable *odt, char *path);
int odt_add_entry_to_parent_dir(OpenDirTable *odt, char *path, OpenDir **od = NULL);
int odt_add_entry(OpenDirTable *odt, char *path, char *child, OpenDir **_od = NULL);
int odt_mkdir(OpenDirTable *odt, char *path, mode_t mode);
int odt_mkdir_base(OpenDirTable *odt);
int odt_rmdir(OpenDirTable *odt, char *path);
int odt_rename_dir(OpenDirTable *odt, char *oldpath, char *newpath);
void odt_record_get_attr(OpenDirTable *odt, char *path);

#endif /* _OPEN_DIR_TABLE_H_ */
