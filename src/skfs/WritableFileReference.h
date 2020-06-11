// WritableFileReference.h

#ifndef _WF_REFERENCE_H_
#define _WF_REFERENCE_H_

/////////////
// includes

#include "AttrCache.h"
#include "AttrWriter.h"
#include "FileBlockWriter.h"


//////////
// types

#ifndef _WF_TYPE_
typedef struct WritableFile WritableFile;
#endif

typedef struct WritableFileReference {
    WritableFile    *wf;
    int                ref;
    int                deleted;
    char            *file;
    int                line;
} WritableFileReference;

WritableFileReference *wfr_new(WritableFile *wf, char *file, int line);
int wfr_delete(WritableFileReference **fr, AttrWriter *aw, FileBlockWriter *fbw, AttrCache *ac);
WritableFile *wfr_get_wf(WritableFileReference *wfr);
void wfr_sanity_check(WritableFileReference *wfr);

#endif
