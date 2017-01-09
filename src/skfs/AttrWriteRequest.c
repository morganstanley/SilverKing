// AttrWriteRequest.c

/////////////
// includes

#include "AttrWriteRequest.h"
#include "Util.h"

#include <string.h>


///////////////////
// implementation

AttrWriteRequest *awr_new(AttrWriter *attrWriter, const char *path, FileAttr *fa) {
	AttrWriteRequest *awr;

	awr = (AttrWriteRequest*)mem_alloc(1, sizeof(AttrWriteRequest));
	awr->attrWriter = attrWriter;
	strncpy(awr->path, path, SRFS_MAX_PATH_LENGTH);
	memcpy(&awr->fa, fa, sizeof(FileAttr));
	return awr;
}

void awr_delete(AttrWriteRequest **awr) {
	if (awr != NULL && *awr != NULL) {
		mem_free((void **)awr);
	} else {
		fatalError("bad ptr in awr_delete");
	}
}
