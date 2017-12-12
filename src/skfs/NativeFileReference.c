// NativeFileReference.c

/////////////
// includes

#include "NativeFileReference.h"
#include "NativeFile.h"
#include "Util.h"


//////////////////
// implementation


NativeFileReference *nfr_new(NativeFile *nf, char *file, int line) {
	NativeFileReference *nfr;
	int	ref;

	ref = nf_create_ref(nf);
	if (ref < 0) {
		fatalError("bogus ref", __FILE__, __LINE__);
	}
	nfr = (NativeFileReference *)mem_alloc(1, sizeof(NativeFileReference));
	nfr->nf = nf;
	nfr->ref = ref;
	nfr->file = file;
	nfr->line = line;
	srfsLog(LOG_FINE, "nfr_new: nfr %llx nf %llx\t%s %d", nfr, nfr->nf, nfr->file, nfr->line);
	return nfr;
}

int nfr_delete(NativeFileReference **nfr, int tableLocked) {
    int rc;
    NativeFile    *nf;
    
    nf = (*nfr)->nf;
    nf_sanity_check(nf);
    rc = 0;
	srfsLog(LOG_FINE, "in nfr_delete %llx", nfr);
	if (nfr != NULL && *nfr != NULL) {
		srfsLog(LOG_FINE, "in2 nfr_delete %llx", *nfr);
		if ((*nfr)->deleted) {
			srfsLog(LOG_ERROR, "multiple deletions %llx", *nfr);
			fatalError("multiple deletions", __FILE__, __LINE__);
		}
		(*nfr)->deleted = TRUE;
		rc = nf_delete_ref((*nfr)->nf, (*nfr)->ref, tableLocked);
		srfsLog(LOG_FINE, "out2 nfr_delete %llx", *nfr);
		mem_free((void **)nfr);
	} else {
		fatalError("bad ptr in nfr_delete");
	}
	srfsLog(LOG_FINE, "out nfr_delete %llx", nfr);
    return rc;
}

NativeFile *nfr_get_nf(NativeFileReference *nfr) {
    return nfr != NULL ? nfr->nf : NULL;
}

int nfr_get_fd(NativeFileReference *nfr) {
    NativeFile  *nf;
    
    nf = nfr_get_nf(nfr);
    return nf != NULL ? nf_get_fd(nf) : -1;
}

void nfr_sanity_check(NativeFileReference *nfr) {
    if (nfr == NULL) {
        fatalError("Unexpected null nfr", __FILE__, __LINE__);
    }
    nf_sanity_check(nfr->nf);
}
