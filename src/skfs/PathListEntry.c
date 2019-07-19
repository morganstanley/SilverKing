// PathListEntry.c

/////////////
// includes

#include "PathListEntry.h"
#include "Util.h"


//////////////
// implementation

PathListEntry *ple_prepend(PathListEntry *existingEntry, char *path) {
    PathListEntry    *newEntry;

    newEntry = (PathListEntry *)mem_alloc(1, sizeof(PathListEntry));
    newEntry->path = path;
    newEntry->next = existingEntry;
    return newEntry;
}
