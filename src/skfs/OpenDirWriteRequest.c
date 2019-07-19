// OpenDirWriteRequest.c

/////////////
// includes

#include "OpenDirWriteRequest.h"
#include "Util.h"

#include <string.h>


///////////////////
// implementation

OpenDirWriteRequest *odwr_new(OpenDirWriter *openDirWriter, OpenDir *od) {
    OpenDirWriteRequest *odwr;

    odwr = (OpenDirWriteRequest*)mem_alloc(1, sizeof(OpenDirWriteRequest));
    odwr->openDirWriter = openDirWriter;
    odwr->od = od;
    return odwr;
}

void odwr_delete(OpenDirWriteRequest **odwr) {
    if (odwr != NULL && *odwr != NULL) {
        mem_free((void **)odwr);
    } else {
        fatalError("bad ptr in odwr_delete");
    }
}
