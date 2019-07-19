// FileBlockWriteRequest.c

/////////////
// includes

#include "FileBlockWriteRequest.h"
#include "Util.h"

#include <stddef.h>
#include <string.h>


///////////////////
// implementation

FileBlockWriteRequest *fbwr_new(FileBlockWriter *fileBlockWriter, FileBlockID *fbid,
                                size_t dataLength, void *data, ActiveOpRef *aor) {
    FileBlockWriteRequest *fbwr;

    fbwr = (FileBlockWriteRequest*)mem_alloc(1, sizeof(FileBlockWriteRequest));
    fbwr->fileBlockWriter = fileBlockWriter;
    fbwr->fbid = fbid_dup(fbid);
    fbwr->dataLength = dataLength;
    fbwr->data = data;
    fbwr->aor = aor;
    return fbwr;
}

void fbwr_delete(FileBlockWriteRequest **fbwr) {
    if (fbwr != NULL && *fbwr != NULL) {
        //srfsLog(LOG_FINE, "fbwr_delete %llx %llx", fbwr, *fbwr);
        if ((*fbwr)->aor != NULL) {
            aor_delete(&(*fbwr)->aor);
        }
        mem_free(&(*fbwr)->data);
        fbid_delete(&(*fbwr)->fbid);
        mem_free((void **)fbwr);
    } else {
        fatalError("bad ptr in fbwr_delete");
    }
}
