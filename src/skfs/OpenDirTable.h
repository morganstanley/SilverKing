// OpenDirTable.h

#ifndef _OPEN_DIR_TABLE_H_
#define _OPEN_DIR_TABLE_H_

/////////////
// includes

#include "AttrReader.h"
#include "AttrWriter.h"
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
} OpenDirTable;


//////////////////////
// public prototypes

OpenDirTable *odt_new(const char *name, SRFSDHT *sd, AttrWriter *aw, AttrReader *ar, ResponseTimeStats *rtsDirData);
void odt_delete(OpenDirTable **odt);
int odt_opendir(OpenDirTable *odt, const char* path, struct fuse_file_info* fi);
int odt_readdir(OpenDirTable *odt, const char *path, void *buf, fuse_fill_dir_t filler,
                       off_t offset, struct fuse_file_info *fi);
int odt_releasedir(OpenDirTable *odt, const char* path, struct fuse_file_info *fi);
int odt_rm_entry_from_parent_dir(OpenDirTable *odt, char *path);
int odt_add_entry_to_parent_dir(OpenDirTable *odt, char *path);
int odt_mkdir(OpenDirTable *odt, char *path, mode_t mode);
int odt_mkdir_base(OpenDirTable *odt);
int odt_rmdir(OpenDirTable *odt, char *path);
int odt_rename_dir(OpenDirTable *odt, char *oldpath, char *newpath);
void odt_record_get_attr(OpenDirTable *odt, char *path);

#endif /* _OPEN_DIR_TABLE_H_ */
