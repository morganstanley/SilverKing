// NativeFileReference.h

#ifndef _NF_REFERENCE_H_
#define _NF_REFERENCE_H_

/////////////
// includes

#include "Util.h"


//////////
// types

#ifndef _NF_TYPE_
typedef struct NativeFile NativeFile;
#endif

typedef struct NativeFileReference {
	NativeFile	*nf;
	int			    ref;
	int			    deleted;
	char		    *file;
	int			    line;
} NativeFileReference;

NativeFileReference *nfr_new(NativeFile *nf, char *file, int line);
int nfr_delete(NativeFileReference **fr, int tableLocked = FALSE);
NativeFile *nfr_get_nf(NativeFileReference *fr);
int nfr_get_fd(NativeFileReference *fr);
void nfr_sanity_check(NativeFileReference *nfr);

#endif
