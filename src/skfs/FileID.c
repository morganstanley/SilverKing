// FileID.c

/////////////
// includes

#include "FileID.h"
#include "SRFSConstants.h"
#include "Util.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

///////////////////
// private defines


///////////////////////
// atomics

// FUTURE - replace this with an intrinsic atomic increment
static inline int atomic_inc(uint64_t *x, pthread_spinlock_t *lock) {
	uint64_t	rVal;
	
	pthread_spin_lock(lock);
	rVal = (*x)++;
	pthread_spin_unlock(lock);
	return rVal;
}


///////////////////////
// private prototypes

static unsigned int _fid_hash(FileID *fid);
static FileID *_fid_generate_new_skfs(uint64_t *nextSequence);

//////////////////////////
// private globals

static pthread_spinlock_t	_fid_sequence_lock;

static uint64_t	_fid_skfs_instance;
static uint64_t	_fid_skfs_nextSequence;
static uint64_t	_fid_skfs_internal_nextSequence;


///////////////////
// implementation

void fid_module_init() {
	if (_fid_skfs_instance != 0) { // loose check
		fatalError("_fid_skfs_instance != 0", __FILE__, __LINE__);
	}
	pthread_spin_init(&_fid_sequence_lock, 0);
	_fid_skfs_instance = (uint64_t)random() ^ (uint64_t)getpid();
	srfsLog(LOG_WARNING, "_fid_skfs_instance %llx", _fid_skfs_instance);
}

void fid_init_native(FileID *fid, struct stat *_stat) {
	fid->fileSystem = fsNative;
	fid->native.inode = _stat->st_ino;
	fid->native.creationTime = _stat->st_ctime;
	fid->native.modTime = _stat->st_mtime;
	fid->native.size = _stat->st_size;
	fid->hash = _fid_hash(fid);
    if (srfsLogLevelMet(LOG_FINE)) {
		char	_fid[SRFS_MAX_PATH_LENGTH];
		
		fid_to_string(fid, _fid);
		srfsLog(LOG_FINE, "%llx %llx  %lu.%lu.%lu.%lu", 
			_stat->st_ino, _stat->st_ctime, _stat->st_mtime, _stat->st_size);
		srfsLog(LOG_FINE, "fid_init_native %llx  %s", fid, _fid);
	}
}

FileID *fid_new_native(struct stat *_stat) {
	FileID	*fid;

	fid = (FileID *)mem_alloc(1, sizeof(FileID));
	srfsLog(LOG_FINE, "fid_new_native %llx", fid);
	fid_init_native(fid, _stat);
	return fid;
}

FileID *fid_dup(FileID *_fid) {
	FileID	*fid;

	fid = (FileID *)mem_alloc(1, sizeof(FileID));
	memcpy(fid, _fid, sizeof(FileID));
	return fid;
}

void fid_init_skfs(FileID *fid, uint64_t instance, uint64_t sequence) {
	fid->fileSystem = fsSKFS;
	fid->skfs.instance = instance;
	fid->skfs.sequence = sequence;
	fid->hash = _fid_hash(fid);
    if (srfsLogLevelMet(LOG_FINE)) {
		char	_fid[SRFS_MAX_PATH_LENGTH];
		
		fid_to_string(fid, _fid);
		srfsLog(LOG_FINE, "%llx %llx", instance, sequence);
		srfsLog(LOG_FINE, "fid_init_skfs %llx  %s", fid, _fid);
	}
}

FileID *fid_new_skfs(uint64_t instance, uint64_t sequence) {
	FileID	*fid;

	fid = (FileID *)mem_alloc(1, sizeof(FileID));
	srfsLog(LOG_FINE, "fid_new_skfs %llx", fid);
	fid_init_skfs(fid, instance, sequence);
	return fid;
}

static FileID *_fid_generate_new_skfs(uint64_t *nextSequence) {
	uint64_t	sequence;
	
	if (_fid_skfs_instance == 0) {
		fatalError("fid_module_init() not called", __FILE__, __LINE__);
	}
	sequence = atomic_inc(nextSequence, &_fid_sequence_lock);
	return fid_new_skfs(_fid_skfs_instance, sequence);
}

FileID *fid_generate_new_skfs() {
	return _fid_generate_new_skfs(&_fid_skfs_nextSequence);
}

FileID *fid_generate_new_skfs_internal() {
	return _fid_generate_new_skfs(&_fid_skfs_internal_nextSequence);
}

static void _fid_generate_and_init_skfs(FileID *fid, uint64_t *nextSequence) {
	uint64_t	sequence;
	
	if (_fid_skfs_instance == 0) {
		fatalError("fid_module_init() not called", __FILE__, __LINE__);
	}
	sequence = atomic_inc(nextSequence, &_fid_sequence_lock);
	fid_init_skfs(fid, _fid_skfs_instance, sequence);
}

void fid_generate_and_init_skfs(FileID *fid) {
	_fid_generate_and_init_skfs(fid, &_fid_skfs_nextSequence);
}

void fid_generate_and_init_skfs_internal(FileID *fid) {
	_fid_generate_and_init_skfs(fid, &_fid_skfs_internal_nextSequence);
}

void fid_delete(FileID **fid) {
	if (fid != NULL && *fid != NULL) {
		srfsLog(LOG_FINE, "fid_delete %llx", *fid);
		mem_free((void **)fid);
	} else {
		fatalError("bad ptr passed to fid_delete");
	}
}

off_t fid_get_size(FileID *fid) {
	if (!fid_is_native_fs(fid)) {
		fatalError("fid_get_size called for non-native fid", __FILE__, __LINE__);
		return 0;
	} else {
		return fid->native.size;
	}
}

unsigned int fid_hash(FileID *fid) {
	return fid->hash;
}

// Note - only safe for hashing a newly minted FileID as it uses the hash in the hash
static unsigned int _fid_hash(FileID *fid) {
	unsigned int	result;

	result = mem_hash(fid, sizeof(FileID));
	srfsLog(LOG_FINE, "_fid_hash\t%llx\t%lu", fid, result);
	return result;
}

int fid_compare(FileID *a, FileID *b) {
	if (a->hash < b->hash) {
		return -1;
	} else if (a->hash > b->hash) {
		return 1;
	} else {
		int	result;

		result = memcmp(a, b, sizeof(FileID));
		srfsLog(LOG_FINE, "fid_compare\t%llx\t%llx\t%d", a, b, result);
		return result;
	}
}

// consider accepting buffer size
// for now we go with the higher performance implementation
int fid_to_string(FileID *fid, char *dest) {
	if (fid_is_native_fs(fid)) {
		return sprintf(dest, "%u.%lu.%lu.%lu.%lu", fid->fileSystem, fid->native.inode, fid->native.creationTime, fid->native.modTime, fid->native.size);
	} else {
		return sprintf(dest, "%u.%lu.%lu", fid->fileSystem, fid->skfs.instance, fid->skfs.sequence);
	}
}

ino_t fid_get_inode(FileID *fid) {
	if (fid_is_native_fs(fid)) {
		return fid->native.inode;
	} else {
		// FUTURE - improve
		return (ino_t)(fid->skfs.instance ^ fid->skfs.sequence);
	}
}
