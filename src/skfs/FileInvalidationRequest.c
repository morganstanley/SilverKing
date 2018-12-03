// FileInvalidationRequest.c

/////////////
// includes

#include "FileInvalidationRequest.h"
#include "Util.h"

#include <stddef.h>
#include <string.h>


///////////////////
// implementation

FileInvalidationRequest *fir_new(FileBlockWriter *fileBlockWriter, FileID *fid,
								int numBlocks) {
	FileInvalidationRequest *fir;

	fir = (FileInvalidationRequest*)mem_alloc(1, sizeof(FileInvalidationRequest));
	fir->fileBlockWriter = fileBlockWriter;
	fir->fid = fid_dup(fid);
    fir->numBlocks = numBlocks;
	return fir;
}

void fir_delete(FileInvalidationRequest **fir) {
	if (fir != NULL && *fir != NULL) {
        //srfsLog(LOG_FINE, "fir_delete %llx %llx", fir, *fir);
		fid_delete(&(*fir)->fid);
		mem_free((void **)fir);
	} else {
		fatalError("bad ptr in fir_delete");
	}
}
