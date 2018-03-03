// OpenDirTable.c

///////////////
// includes

#include "DirData.h"
#include "DirEntry.h"
#include "OpenDir.h"
#include "OpenDirTable.h"
#include "ReconciliationSet.h"

#include <errno.h>


////////////
// defines

#define ODT_ODC_NAME	"OpenDirCache"
#define ODT_ODC_CACHE_SIZE	0
#define ODT_ODC_SUB_CACHES	8	
#define ODT_ODC_CACHE_EVICTION_BATCH	0
#define ODT_TRIGGER_INTERVAL_MILLIS	10
// FUTURE - limit ODC cache size in the future, and add an eviction batch size such as 128

#define _ODT_OPENDIR_RETRIES    10
#define _ODT_OPENDIR_RETRY_INTERVAL_SECONDS 1


////////////
// externs

extern OpenDirWriter	*od_odw;


/////////////////
// private data

//static uint64_t _minReconciliationSleepMillis = 1 * 1000;
//static uint64_t	_maxReconciliationSleepMillis = 8 * 1000;
static uint64_t _minMinReconciliationSleepMillis = 1;
static uint64_t	_maxMaxReconciliationSleepMillis = 1 * 60 * 1000;
static uint64_t _minReconciliationSleepMillis = 800;
static uint64_t	_maxReconciliationSleepMillis = 1 * 1000;
static unsigned int _reconciliationSeed;


///////////////////////
// private prototypes

static void odt_set_reconciliation_sleep(OpenDirTable *odt, char *reconciliationSleep);

static void odt_fetch_all_attr(OpenDirTable *odt, OpenDir *od);
static void *odt_od_reconciliation_run(void *_odt);

static int odt_rm_entry(OpenDirTable *odt, char *path, char *child);

static uint64_t odt_getVersion(OpenDirTable *odt);
int _odt_mkdir_base(OpenDirTable *odt, char *path, mode_t mode);

///////////////
// implementation

OpenDirTable *odt_new(const char *name, SRFSDHT *sd, AttrWriter *aw, AttrReader *ar, ResponseTimeStats *rtsDirData, char *reconciliationSleep, uint64_t odwMinWriteIntervalMillis) {
	OpenDirTable	*odt;
	int	i;
	CacheStoreResult	result;

	odt = (OpenDirTable *)mem_alloc(1, sizeof(OpenDirTable));
    srfsLog(LOG_FINE, "odt_new:\t%s\n", name);
    odt->name = name;
	odt->odc = odc_new(ODT_ODC_NAME, ODT_ODC_CACHE_SIZE, ODT_ODC_CACHE_EVICTION_BATCH, ODT_ODC_SUB_CACHES);
	odt->ddr = ddr_new(sd, rtsDirData, odt->odc);
	odt->odw = odw_new(sd/*, odt->ddr*/, odwMinWriteIntervalMillis);
	odt->aw = aw;
	odt->ar = ar;
	
	od_odw = odt->odw; // FUTURE - remove need for the global
	
	_reconciliationSeed = (unsigned int)curTimeMillis() ^ (unsigned int)(uint64_t)odt;
	pthread_create(&odt->reconciliationThread, NULL, odt_od_reconciliation_run, odt);
    
    odt_set_reconciliation_sleep(odt, reconciliationSleep);
    srfsLog(LOG_WARNING, "odt->minReconciliationSleepMillis %lu odt->maxReconciliationSleepMillis %lu\n", 
            odt->minReconciliationSleepMillis, odt->maxReconciliationSleepMillis);
	
    odt->_lastVersion = 0;
    if (pthread_spin_init(&odt->lvLock, PTHREAD_PROCESS_PRIVATE) != 0) {
        fatalError("Unable to initialize lvLock in odt_new()");
    }
    
	return odt;
}

static void odt_set_reconciliation_sleep(OpenDirTable *odt, char *reconciliationSleep) {
    uint64_t    _min;
    uint64_t    _max;
    
    _min = _minReconciliationSleepMillis;
    _max = _maxReconciliationSleepMillis;
    if (reconciliationSleep != NULL) {
        char    *c;
        
        c = strchr(reconciliationSleep, ',');
        if (c != NULL) {
            *c = '\0';
            _min = strtoull(reconciliationSleep, NULL, 10);
            _max = strtoull(c + 1, NULL, 10);
        }
    }
    if (_min < _minMinReconciliationSleepMillis) {
        _min = _minMinReconciliationSleepMillis;
    }
    if (_max > _maxMaxReconciliationSleepMillis) {
        _max = _maxMaxReconciliationSleepMillis;
    }
    odt->minReconciliationSleepMillis = _min;
    odt->maxReconciliationSleepMillis = _max;
}

void odt_delete(OpenDirTable **odt) {
	if (odt != NULL && *odt != NULL) {
		odc_delete(&(*odt)->odc);
		ddr_delete(&(*odt)->ddr);
		mem_free((void **)odt);
	} else {
		fatalError("bad ptr in odt_delete");
	}
}

/**
 * Read directory data
 */
DirData *odt_get_DirData(OpenDirTable *odt, char *path) {
    DirData *dd;
    
    srfsLog(LOG_FINE, "odt_get_DirData");
	dd = ddr_get_DirData(odt->ddr, path);
    if (dd != NULL) {
        srfsLog(LOG_FINE, "dd->numEntries", dd->numEntries);
    } else {
        srfsLog(LOG_WARNING, "No DirData for %s", path);
    }
    return dd;
}

//int odt_get_dir_id(OpenDirTable *odt, const char* path, char *dirID) {
//    int result;
//    FileAttr    fa;
//    
//    memset(&fa, 0, sizeof(FileAttr));
//    result = ar_get_attr(odt->ar, (char *)path, &fa);
//    if (result == 0) {
//        fid_to_string(&fa.fid, dirID);
//    }
//    return result;
//}

int odt_opendir(OpenDirTable *odt, const char* path, struct fuse_file_info* fi) {
	DirData	*openDir;
    int     attempt;
    int     result;
	
    attempt = 0;
    result = 0;
    do {
        openDir = odt_get_DirData(odt, (char *)path);
        if (openDir) {
            if (fi) {
                fi->fh = (uint64_t)openDir;
            }
            result = 0;
        } else {
            srfsLog(LOG_INFO, "odt_opendir can't find %s", path);
            result = -ENOENT;
        }
        if (result != 0) {
            srfsLog(LOG_WARNING, "odt_opendir error %s %d", path, result);
            if (attempt < _ODT_OPENDIR_RETRIES) {
                sleep(_ODT_OPENDIR_RETRY_INTERVAL_SECONDS);
            }
        }
    } while (result != 0 && attempt++ < _ODT_OPENDIR_RETRIES);
    return result;
}

int odt_readdir(OpenDirTable *odt, const char *path, void *buf, fuse_fill_dir_t filler,
                       off_t offset, struct fuse_file_info *fi) {
	DirData	*dd;
	
    srfsLog(LOG_FINE, "odt_readdir %s %d", path, offset);
	dd = dd_fuse_fi_fh_to_dd(fi);
	if (dd == NULL) {
		srfsLog(LOG_ERROR, "dr_readdir ignoring bogus fi->fh");
		return -EIO;
	} else {
		DirEntry	*de;
		DirEntry	*limit;
		
        limit = (DirEntry *)offset_to_ptr(dd->data, dd->indexOffset);
		de = de_initial((const char *)(dd->data + offset), limit);
		while (de) {
			DirEntry	*deNext;
			off_t		nextOffset;
		
			deNext = de_next(de, limit);
			if (deNext) {
				nextOffset = (off_t)deNext - (off_t)dd->data;
			} else {
				nextOffset = 0;
			}
			srfsLog(LOG_FINE, "de %llx deNext %llx curOffset %d nextOffset %llx %d", de, deNext, (uint64_t)de - (uint64_t)dd->data, nextOffset, nextOffset);
			//if (filler(buf, de_get_name(de), NULL, nextOffset)) {
            //srfsLog(LOG_WARNING, "de_get_name(de) %s %d", de_get_name(de), de_is_deleted(de));
            if (!de_is_deleted(de)) {
                if (filler(buf, de_get_name(de), NULL, 0)) { // ignore offsets for now
                    return 0;
                }			
            }
			de = deNext;
		}
		return 0;
	}
}

int odt_releasedir(OpenDirTable *odt, const char* path, struct fuse_file_info *fi) {
	DirData	*dd;
	
	dd = dd_fuse_fi_fh_to_dd(fi);
	if (dd != NULL) {
		dd_delete(&dd);
	} else {
		srfsLog(LOG_INFO, "dr_releasedir ignoring bogus fi->fh");
	}
	return 0;
}

static uint64_t odt_getVersion(OpenDirTable *odt) {
    uint64_t    v;
    
    v = curTimeMicros();
    // a simplistic attempt to reduce collisions without significantly impacting speed
    if (pthread_spin_lock(&odt->lvLock) != 0) {
        fatalError("Unable to lock lvLock in odt_getVersion()");
    }
    if (v <= odt->_lastVersion) {
        odt->_lastVersion++;
        v = odt->_lastVersion;
        if (pthread_spin_unlock(&odt->lvLock) != 0) {
            fatalError("Unable to unlock lvLock in odt_getVersion()");
        }
    } else {
        odt->_lastVersion = v;
        if (pthread_spin_unlock(&odt->lvLock) != 0) {
            fatalError("Unable to unlock lvLock in odt_getVersion()");
        }
    }
    return v;
}

static int odt_rm_entry(OpenDirTable *odt, char *path, char *child) {
	int	result;
	OpenDir	*od;
    uint64_t    version;

    version = odt_getVersion(odt);
	srfsLog(LOG_FINE, "odt_rm_entry %s %s", path, child);
	result = ddr_get_OpenDir(odt->ddr, path, &od, DDR_NO_AUTO_CREATE);
	if (result == 0) {
		if (srfsLogLevelMet(LOG_FINE)) {
			srfsLog(LOG_WARNING, "od before removal");
			od_display(od, stderr);
		}
		od_rm_entry(od, child, version);
		if (srfsLogLevelMet(LOG_FINE)) {
			srfsLog(LOG_WARNING, "od after removal");
			od_display(od, stderr);
		}
		odw_write_dir(odt->odw, path, od);
	}
	return result;
}

int odt_add_entry(OpenDirTable *odt, char *path, char *child, OpenDir **_od) {
	int	result;
	OpenDir	*od;
    uint64_t    version;

    version = odt_getVersion(odt);
	srfsLog(LOG_FINE, "odt_add_entry %s %s", path, child);
    od = NULL;
    // We auto-create below as a safety check. The parent
    // should exist and creation should not be necessary, but
    // loose consistency of dirs may mean that we don't see it yet.
	result = ddr_get_OpenDir(odt->ddr, path, &od, DDR_AUTO_CREATE);
	if (result == 0) {
        if (_od != NULL) {
            *_od = od;
        }
		if (srfsLogLevelMet(LOG_FINE)) {
			srfsLog(LOG_WARNING, "od before addition");
			od_display(od, stderr);
		}
		od_add_entry(od, child, version);
		if (srfsLogLevelMet(LOG_FINE)) {
			srfsLog(LOG_WARNING, "od after addition");
			od_display(od, stderr);
		}
		odw_write_dir(odt->odw, path, od);
	}
	return result;
}

int odt_add_entry_to_parent_dir(OpenDirTable *odt, char *path, OpenDir **od) {
	char	*lastSlash;
	
	lastSlash = strrchr(path, '/');
	if (lastSlash == NULL) {
		return EIO;
	} else {
		char	parent[SRFS_MAX_PATH_LENGTH];
		
		memset(parent, 0, SRFS_MAX_PATH_LENGTH);
		memcpy(parent, path, (size_t)(lastSlash - path));
		if (TRUE) { // Placeholder for possible future filter on directories (previously /skfs was filtered)
			char	*child;
			
			child = lastSlash + 1;
			return odt_add_entry(odt, parent, child, od);
		} else {
			return 0;
		}
	}
}

int odt_rm_entry_from_parent_dir(OpenDirTable *odt, char *path) {
	char	*lastSlash;
	
	lastSlash = strrchr(path, '/');
	if (lastSlash == NULL) {
		return EIO;
	} else {
		char	parent[SRFS_MAX_PATH_LENGTH];
		
		memset(parent, 0, SRFS_MAX_PATH_LENGTH);
		memcpy(parent, path, (size_t)(lastSlash - path));
		if (TRUE) { // Placeholder for possible future filter on directories (previously /skfs was filtered)
			char	*child;
			
			child = lastSlash + 1;
			return odt_rm_entry(odt, parent, child);
		} else {
			return 0;
		}
	}
}

int odt_mkdir_base(OpenDirTable *odt) {
    int result;
    
    result = _odt_mkdir_base(odt, SKFS_BASE, 0555);
    if (result != 0) {
        return result;
    }
    result = _odt_mkdir_base(odt, SKFS_WRITE_BASE, 0777);
    return result;
}

int _odt_mkdir_base(OpenDirTable *odt, char *path, mode_t mode) {
	int	result;
	FileAttr	fa;
	time_t	curEpochTimeSeconds;
	SKOperationState::SKOperationState	awResult;
	struct fuse_context	*fuseContext;	
	
	fuseContext = fuse_get_context();
	
	srfsLog(LOG_FINE, "in odt_mkdir_base %s %o", path, mode);
	memset(&fa, 0, sizeof(struct FileAttr));
	
	fid_generate_and_init_skfs(&fa.fid);
	
	// Fill in file stat information
	fa.stat.st_mode = S_IFDIR | (mode & 0777);
	srfsLog(LOG_FINE, "mode %o", fa.stat.st_mode);
	fa.stat.st_nlink = 1;
	fa.stat.st_ino = fid_get_inode(&fa.fid);
	curEpochTimeSeconds = epoch_time_seconds();
	fa.stat.st_ctime = curEpochTimeSeconds;
	fa.stat.st_mtime = curEpochTimeSeconds;
	fa.stat.st_atime = curEpochTimeSeconds;
	fa.stat.st_uid = fuseContext->uid;
	fa.stat.st_gid = fuseContext->gid;
	fa.stat.st_blksize = SRFS_BLOCK_SIZE;
	fa.stat.st_blocks = 1; // FUTURE - consider using a real size
	fa.stat.st_size = 1; // FUTURE - consider using a real size

	//aw_write_attr(aw, wf->path, &wf->fa); // old queued async write
	// Write out attribute information & wait for the write to complete
	awResult = aw_write_attr_direct(odt->aw, path, &fa, ar_get_attrCache(odt->ar));
	if (awResult != SKOperationState::SUCCEEDED) {
		srfsLog(LOG_WARNING, "odt_mkdir aw_write_attr_direct failed %s", path);
	}
	
	OpenDir	*od;

	od = NULL;
	result = ddr_get_OpenDir(odt->ddr, path, &od, DDR_AUTO_CREATE);
	srfsLog(LOG_FINE, "odt_mkdir_base ddr_get_OpenDir %s %d", path, result);
	srfsLog(LOG_FINE, "out odt_mkdir_base %s %d", path, -result);	
	return -result;
}

int odt_mkdir(OpenDirTable *odt, char *path, mode_t mode) {
	int	result;
	FileAttr	fa;
	time_t	curEpochTimeSeconds;
	SKOperationState::SKOperationState	awResult;
	struct fuse_context	*fuseContext;	

	fuseContext = fuse_get_context();
	
	srfsLog(LOG_FINE, "in odt_mkdir %s %o", path, mode);
	memset(&fa, 0, sizeof(struct FileAttr));
	
	result = odt_add_entry_to_parent_dir(odt, path);
	if (result != 0) {
		return -result;
	}
	
	fid_generate_and_init_skfs(&fa.fid);
	
	// Fill in file stat information
	fa.stat.st_mode = S_IFDIR | (mode & 0777);
	srfsLog(LOG_FINE, "mode %o", fa.stat.st_mode);
	fa.stat.st_nlink = 1;
	fa.stat.st_ino = fid_get_inode(&fa.fid);
	curEpochTimeSeconds = epoch_time_seconds();
	fa.stat.st_ctime = curEpochTimeSeconds;
	fa.stat.st_mtime = curEpochTimeSeconds;
	fa.stat.st_atime = curEpochTimeSeconds;
	fa.stat.st_uid = fuseContext->uid;
	fa.stat.st_gid = fuseContext->gid;
	fa.stat.st_blksize = SRFS_BLOCK_SIZE;
	fa.stat.st_blocks = 1; // FUTURE - consider using a real size
	fa.stat.st_size = 1; // FUTURE - consider using a real size

	//aw_write_attr(aw, wf->path, &wf->fa); // old queued async write
	// Write out attribute information & wait for the write to complete
	awResult = aw_write_attr_direct(odt->aw, path, &fa, ar_get_attrCache(odt->ar));
	if (awResult != SKOperationState::SUCCEEDED) {
        FileAttr    fa;
        int         gaResult;
        
        memset(&fa, 0, sizeof(FileAttr));
        gaResult = ar_get_attr(odt->ar, path, &fa);
        if (!gaResult) {
            srfsLog(LOG_WARNING, "odt_mkdir aw_write_attr_direct failed / EEXIST %s %d %d %x", path, awResult, gaResult, fa.stat.st_mode);
            // FUTURE - change to LOG_INFO - LOG_WARNING for verification only
            result = EEXIST;
        } else {
            int rmpResult;
            
            srfsLog(LOG_WARNING, "odt_mkdir aw_write_attr_direct failed2 %s %d %d", path, awResult, gaResult);
            result = EIO;
            
            // Remove this entry from the parent since the aw failed
            rmpResult = odt_rm_entry_from_parent_dir(odt, path);
            if (rmpResult != 0) {
                srfsLog(LOG_WARNING, "odt_mkdir aw_write_attr_direct failed and can't remove from parent. %s %d %d", path, awResult, gaResult);
            }
        }
	} else {
		OpenDir	*od;
	
		od = NULL;
		result = ddr_get_OpenDir(odt->ddr, path, &od, DDR_AUTO_CREATE);
		srfsLog(LOG_FINE, "odt_mkdir ddr_get_OpenDir %s %d", path, result);
		if (result == 0) {
			odw_write_dir(odt->odw, path, od);
		} else {
            srfsLog(LOG_WARNING, "odt_mkdir ddr_get_OpenDir failed %s %d", path, result);
			odw_write_dir(odt->odw, path, od);
        }
	}
	srfsLog(LOG_FINE, "out odt_mkdir %s %d", path, -result);	
	return -result;
}

int odt_rmdir(OpenDirTable *odt, char *path) {
    DirData *dd;
	
	srfsLog(LOG_FINE, "in odt_rmdir %s", path);    
	dd = ddr_get_DirData(odt->ddr, path);
    if (dd == NULL) {
        srfsLog(LOG_WARNING, "null ddr_get_DirData %s", path);    
        return -EIO;
    }
    if (!dd_is_empty(dd)) {
        return -ENOTEMPTY;
    } else {
        int     result;
        FileAttr	fa;
        time_t	curEpochTimeSeconds;
        SKOperationState::SKOperationState	awResult;
        //struct fuse_context	*fuseContext;	
        //fuseContext = fuse_get_context();
    
        memset(&fa, 0, sizeof(struct FileAttr));
        
        result = odt_rm_entry_from_parent_dir(odt, path);
        if (result != 0) {
            return -result;
        }
        
        fid_generate_and_init_skfs(&fa.fid);
        
        // Fill in file stat information
        srfsLog(LOG_FINE, "mode %o", fa.stat.st_mode);
        fa.stat.st_nlink = 0;

        //aw_write_attr(aw, wf->path, &wf->fa); // old queued async write
        // Write out attribute information & wait for the write to complete
        awResult = aw_write_attr_direct(odt->aw, path, &fa, ar_get_attrCache(odt->ar));
        if (awResult != SKOperationState::SUCCEEDED) {
            srfsLog(LOG_WARNING, "odt_mkdir aw_write_attr_direct failed %s", path);
            result = EIO;
        } else {
            OpenDir	*od;
        
            od = NULL;
            result = ddr_get_OpenDir(odt->ddr, path, &od, DDR_AUTO_CREATE);
            srfsLog(LOG_FINE, "odt_rmdir ddr_get_OpenDir %s %d", path, result);
            if (result == 0) {
                od_mark_deleted(od);
            }
        }
        srfsLog(LOG_FINE, "out odt_mkdir %s %d", path, -result);	
        return -result;
    }
}

/*
Renaming directories is fundamentally more difficult than this in the current
implementation. This would need to operate recursively if we were to use it.
For now, simply disallow.
int odt_rename_dir(OpenDirTable *odt, char *oldpath, char *newpath, FileAttr *newFa) {
    int result;
    
    srfsLog(LOG_FINE, "odt_rename_dir %s %s", oldpath, newpath);
    // Create the new directory
    result = odt_mkdir(odt, newpath, newFa->stat.st_mode);
    if (result != 0) {
        return result;
    } else {
        // Copy entries from the old to the new directory
        OpenDir     *od;

        result = ddr_get_OpenDir(odt->ddr, newpath, &od, DDR_NO_AUTO_CREATE);
        if (result != 0) {
            // mkdir should have created the OpenDir
            fatalError("panic", __FILE__, __LINE__);
            return -result;
        } else {
            uint32_t    i;
            DirData     *dd;
            
            dd = od_get_DirData(od, TRUE);
            for (i = 0; i < dd->numEntries; i++) {
                DirEntry    *de;
                
                de = dd_get_entry(dd, i);
                if (de == NULL) {
                    fatalError("panic", __FILE__, __LINE__);
                }
                if (!de_is_deleted(de)) {
                    od_add_entry(od, (char *)de_get_name(de), de->version);
                } else {
                    od_rm_entry(od, (char *)de_get_name(de), de->version);
                }
            }
            dd_delete(&dd);
            odw_write_dir(odt->odw, newpath, od);
            
            // Delete the old directory
            return odt_rmdir(odt, oldpath);
        }
    }
}
*/


void odt_record_get_attr(OpenDirTable *odt, char * path) {
	uint64_t	curTime;
	
	curTime = curTimeMillis();
	if (curTime - odt->lastGetAttr <= ODT_TRIGGER_INTERVAL_MILLIS) {
		char	*lastSlash;
		
		lastSlash = strrchr(path, '/');
		if (lastSlash == NULL || lastSlash == path) {
			// no parent, ignore
		} else {
			char	parent[SRFS_MAX_PATH_LENGTH];
			char	*child;
			OpenDir	*od;
			int		result;
				
			memset(parent, 0, SRFS_MAX_PATH_LENGTH);
			memcpy(parent, path, (size_t)(lastSlash - path));
			child = lastSlash + 1;

			od = NULL;
			result = ddr_get_OpenDir(odt->ddr, parent, &od, DDR_NO_AUTO_CREATE);
			if (result == 0) {
				if (!od) {
					srfsLog(LOG_INFO, "NULL od for %s %s", parent, child);
				} else {
					int	fetchTriggered;
					
					fetchTriggered = od_record_get_attr(od, child, curTime);
					if (fetchTriggered) {
						srfsLog(LOG_WARNING, "Attr prefetch triggered: %s", od->path);
						odt_fetch_all_attr(odt, od);
					}
				}
			}
		}
	}
	odt->lastGetAttr = curTime;
}

static void odt_fetch_all_attr(OpenDirTable *odt, OpenDir *od) {
	DirData		*dd;
	DirEntry	*de;
	DirEntry	*limit;
	off_t 		offset;

	offset = 0;
	dd = od_get_DirData(od);
	limit = (DirEntry *)(dd->data + dd->dataLength);
	de = de_initial((const char *)(dd->data + offset), limit);
	while (de) {
		DirEntry	*deNext;
		off_t		nextOffset;

		deNext = de_next(de, limit);
		if (deNext) {
			nextOffset = (off_t)deNext - (off_t)dd->data;
		} else {
			nextOffset = 0;
		}
		srfsLog(LOG_FINE, "de %llx deNext %llx curOffset %d nextOffset %llx %d", de, deNext, (uint64_t)de - (uint64_t)dd->data, nextOffset, nextOffset);
		
		ar_prefetch(odt->ar, od->path, (char *)de_get_name(de));
		
		de = deNext;
	}
	dd_delete(&dd);
}

static void odt_od_reconciliation(OpenDirTable *odt) {
	srfsLog(LOG_INFO, "in odt_od_reconciliation");
	std::set<std::string> rcst;
	
	rcst = rcst_get_current_set();
	for (std::set<std::string>::iterator it = rcst.begin(); it != rcst.end(); ++it) {
		srfsLog(LOG_INFO, "ddr_check_for_reconciliation %s", it->c_str());
		ddr_check_for_reconciliation(odt->ddr, (char *)it->c_str());
	}
	srfsLog(LOG_INFO, "out odt_od_reconciliation");
}

static void *odt_od_reconciliation_run(void *_odt) {
	OpenDirTable	*odt;
	int				running;
	
	odt = (OpenDirTable *)_odt;
	srfsLog(LOG_FINE, "Starting odt_od_reconciliation_run");
	running = TRUE;
	while (running) {
		sleep_random_millis(odt->minReconciliationSleepMillis, odt->maxReconciliationSleepMillis, &_reconciliationSeed);
		odt_od_reconciliation(odt);
	}
    return NULL;
}
