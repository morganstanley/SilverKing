// SKFSOpenFile.c

/////////////
// includes

#include "SKFSOpenFile.h"


////////////
// defines

#define SOF_MAGIC   0xdabc


///////////////////
// implementation

SKFSOpenFile *sof_new() {
	SKFSOpenFile    *sof;

	sof = (SKFSOpenFile*)mem_alloc(1, sizeof(SKFSOpenFile));
    sof->magic = SOF_MAGIC;
    return sof;
}

void sof_delete(SKFSOpenFile **sof) {
	if (sof != NULL && *sof != NULL) {
        if ((*sof)->attr != NULL) {
            fa_delete(&(*sof)->attr);
        }
        if ((*sof)->nativePath != NULL) {
            mem_free((void **)&(*sof)->nativePath);
        }
		mem_free((void **)sof);
	} else {
		fatalError("bad ptr in sof_delete");
	}
}

int sof_is_valid(SKFSOpenFile *sof) {
    return sof != NULL && sof->magic == SOF_MAGIC;
}
