// WritableWritableFileReference.c

/////////////
// includes

#include "WritableFileReference.h"
#include "WritableFile.h"
#include "Util.h"


//////////////////
// iplementation


WritableFileReference *wfr_new(WritableFile *wf, char *file, int line) {
    WritableFileReference *wfr;
    int    ref;

    ref = wf_create_ref(wf);
    if (ref < 0) {
        fatalError("bogus ref", __FILE__, __LINE__);
    }
    wfr = (WritableFileReference *)mem_alloc(1, sizeof(WritableFileReference));
    wfr->wf = wf;
    wfr->ref = ref;
    wfr->file = file;
    wfr->line = line;
    srfsLog(LOG_FINE, "wfr_new: wfr %llx wf %llx\t%s %d", wfr, wfr->wf, wfr->file, wfr->line);
    return wfr;
}

int wfr_delete(WritableFileReference **wfr, AttrWriter *aw, FileBlockWriter *fbw, AttrCache *ac) {
    int rc;
    WritableFile    *wf;
    
    wf = (*wfr)->wf;
    wf_sanity_check(wf);
    rc = 0;
    srfsLog(LOG_FINE, "in wfr_delete %llx", wfr);
    if (wfr != NULL && *wfr != NULL) {
        srfsLog(LOG_FINE, "in2 wfr_delete %llx", *wfr);
        if ((*wfr)->deleted) {
            srfsLog(LOG_ERROR, "multiple deletions %llx", *wfr);
            fatalError("multiple deletions", __FILE__, __LINE__);
        }
        (*wfr)->deleted = TRUE;
        rc = wf_delete_ref((*wfr)->wf, (*wfr)->ref, aw, fbw, ac);
        srfsLog(LOG_FINE, "out2 wfr_delete %llx", *wfr);
        mem_free((void **)wfr);
    } else {
        fatalError("bad ptr in wfr_delete");
    }
    srfsLog(LOG_FINE, "out wfr_delete %llx", wfr);
    return rc;
}

WritableFile *wfr_get_wf(WritableFileReference *wfr) {
    return wfr != NULL ? wfr->wf : NULL;
}

void wfr_sanity_check(WritableFileReference *wfr) {
    if (wfr == NULL) {
        fatalError("Unexpected null wfr", __FILE__, __LINE__);
    }
    wf_sanity_check(wfr->wf);
}
