// OpenDirUpdate.c

///////////////
// includes

#include "OpenDirUpdate.h"
#include "Util.h"

#include <errno.h>


///////////////
// implementation

void odu_init(OpenDirUpdate *odu, uint32_t type, uint64_t version, char *name) {
    odu->type = type;
    odu->version = version;
    odu->name = str_dup(name);
}

void odu_delete(OpenDirUpdate **odu) {
    if (odu != NULL && *odu != NULL) {
        if ((*odu)->name != NULL) {
            mem_free((void **)&(*odu)->name);
        }
        mem_free((void **)odu);
    } else {
        fatalError("bad ptr in odu_delete");
    }
}

void odu_modify(OpenDirUpdate *odu, uint32_t type, uint64_t version) {
    odu->type = type;
    odu->version = version;
}
