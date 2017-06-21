// AttrReadRequest.c

/////////////
// includes

#include "AttrReadRequest.h"
#include "Util.h"

#include <string.h>


///////////////////
// implementation

AttrReadRequest *arr_new(AttrReader *attrReader, char *path, uint64_t minModificationTimeMicros) {
	AttrReadRequest *arr;

	arr = (AttrReadRequest*)mem_alloc(1, sizeof(AttrReadRequest));
	arr->attrReader = attrReader;
	arr->path = (char *)mem_alloc(strlen(path) + 1, 1);
    arr->minModificationTimeMicros = minModificationTimeMicros;
	strcpy(arr->path, path);
	return arr;
}

void arr_delete(AttrReadRequest **arr) {
	if (arr != NULL && *arr != NULL) {
		mem_free((void **)&(*arr)->path);
		mem_free((void **)arr);
	} else {
		fatalError("bad ptr in arr_delete");
	}
}

void arr_display(AttrReadRequest *arr, LogLevel level) {
	srfsLog(level, "%llx attrReader %llx path %llx", arr, arr->attrReader, arr->path);
}
