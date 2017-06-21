// FileBlockReadRequest.c

/////////////
// includes

#include "FileBlockReadRequest.h"

#include "Util.h"


///////////////////
// implementation

FileBlockReadRequest *fbrr_new(FileBlockReader *fileBlockReader, FileBlockID *fbid,
                               uint64_t minModificationTimeMicros) {
	FileBlockReadRequest *fbrr;

	fbrr = (FileBlockReadRequest*)mem_alloc(1, sizeof(FileBlockReadRequest));
	fbrr->fileBlockReader = fileBlockReader;
	fbrr->fbid = fbid_dup(fbid);
    fbrr->minModificationTimeMicros = minModificationTimeMicros;
	return fbrr;
}

void fbrr_delete(FileBlockReadRequest **fbrr) {
	if (fbrr != NULL && *fbrr != NULL) {
		fbid_delete(&(*fbrr)->fbid);
		mem_free((void **)fbrr);
	} else {
		fatalError("bad ptr in fbrr_delete");
	}
}

void fbrr_display(FileBlockReadRequest *fbrr, LogLevel level) {
	srfsLog(level, "fbrr@%lxx", fbrr);
}
