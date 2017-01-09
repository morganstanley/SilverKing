// DirDataReadRequest.c

/////////////
// includes

#include "DirDataReadRequest.h"
#include "Util.h"

#include <string.h>


///////////////////
// implementation

DirDataReadRequest *ddrr_new(DirDataReader *dirDataReader, char *path, DDRR_Type type) {
	DirDataReadRequest *ddrr;

	ddrr = (DirDataReadRequest*)mem_alloc(1, sizeof(DirDataReadRequest));
	ddrr->dirDataReader = dirDataReader;
	ddrr->path = (char *)mem_alloc(strlen(path) + 1, 1);
	strcpy(ddrr->path, path);
	ddrr->type = type;
	return ddrr;
}

void ddrr_delete(DirDataReadRequest **ddrr) {
	if (ddrr != NULL && *ddrr != NULL) {
		mem_free((void **)&(*ddrr)->path);
		mem_free((void **)ddrr);
	} else {
		fatalError("bad ptr in ddrr_delete");
	}
}

void ddrr_display(DirDataReadRequest *ddrr, LogLevel level) {
	srfsLog(level, "%llx DirDataReader %llx path %llx type %d", ddrr, ddrr->dirDataReader, ddrr->path, ddrr->type);
}
