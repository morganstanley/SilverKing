// NativeFile.h

#ifndef _NATIVE_FILE_H_
#define _NATIVE_FILE_H_

/////////////
// includes

#include "HashTableAndLock.h"
#include "NativeFileReference.h"
#include "Util.h"

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>


////////////
// defines

#define NFR_MAX_REFS    128
#define NFR_RECYCLE_THRESHOLD    64
// NFR_RECYCLE_THRESHOLD must be < NFR_MAX_REFS for reference number recycling to be active


//////////
// types

typedef enum {NFR_Invalid = 0, NFR_Created, NFR_Destroyed} NFRefStatus;

typedef struct NativeFileReferentState {
	NFRefStatus	    refStatus[NFR_MAX_REFS];
	int				nextRef;
	int				toDelete;
} NativeFileReferentState;


#define _NF_TYPE_
typedef struct NativeFile {
	uint16_t	magic;	
    const char  *path;
    int         fd;
	pthread_mutex_t lock;
    HashTableAndLock    *htl;
    NativeFileReferentState   referentState;
} NativeFile;


//////////////////////
// public prototypes

NativeFile *nf_new(const char *path, int fd, HashTableAndLock *htl);
void nf_delete(NativeFile **nf);
NativeFileReference *nf_add_reference(NativeFile *nf, char *file, int line);
int nf_create_ref(NativeFile *nf);
int nf_delete_ref(NativeFile *nf, int ref, int tableLocked = FALSE);
int nf_get_fd(NativeFile *nf);
void nf_sanity_check(NativeFile *nf);

#endif
