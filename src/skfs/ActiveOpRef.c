// ActiveOpRef.c

/////////////
// includes

#include "ActiveOp.h"
#include "ActiveOpRef.h"
#include "Util.h"


//////////////////
// iplementation


ActiveOpRef *aor_new(ActiveOp *ao, char *file, int line) {
    ActiveOpRef *aor;
    int    ref;

    ref = ao_create_ref(ao);
    if (ref < 0) {
        fatalError("bogus ref", __FILE__, __LINE__);
    }
    aor = (ActiveOpRef *)mem_alloc(1, sizeof(ActiveOpRef));
    aor->ao = ao;
    aor->ref = ref;
    aor->file = file;
    aor->line = line;
    if (srfsLogLevelMet(LOG_FINE)) {
        srfsLog(LOG_FINE, "aor_new: aor %llx ao %llx\t%s %d", aor, aor->ao, aor->file, aor->line);
    }
    return aor;
}

void aor_delete(ActiveOpRef **aor) {
    if (srfsLogLevelMet(LOG_FINE)) {
        srfsLog(LOG_FINE, "in aor_delete %llx", aor);
    }
    if (aor != NULL && *aor != NULL) {
        if (srfsLogLevelMet(LOG_FINE)) {
            srfsLog(LOG_FINE, "in2 aor_delete %llx", *aor);
        }
        if ((*aor)->deleted) {
            srfsLog(LOG_ERROR, "multiple deletions %llx", *aor);                
            fatalError("multiple deletions", __FILE__, __LINE__);
        }
        (*aor)->deleted = TRUE;
        if (srfsLogLevelMet(LOG_FINE)) {
            srfsLog(LOG_FINE, "aor_delete from %llx %s %d", *aor, (*aor)->file, (*aor)->line);
        }
        ao_delete_ref((*aor)->ao, (*aor)->ref);
        if (srfsLogLevelMet(LOG_FINE)) {
            srfsLog(LOG_FINE, "out2 aor_delete %llx", *aor);
        }
        mem_free((void **)aor);
    } else {
        fatalError("bad ptr in aor_delete");
    }
    if (srfsLogLevelMet(LOG_FINE)) {
        srfsLog(LOG_FINE, "out aor_delete %llx", aor);
    }
}

void *aor_get_target(ActiveOpRef *aor) {
    return ao_get_target(aor->ao);
}

void *aor_get_rVal(ActiveOpRef *aor) {
    return ao_get_rVal(aor->ao);
}

size_t aor_get_rValLength(ActiveOpRef *aor) {
    return ao_get_rValLength(aor->ao);
}

AOResult aor_wait_for_completion(ActiveOpRef *aor) {
    return ao_wait_for_completion(aor->ao);
}

AOResult aor_wait_for_stage(ActiveOpRef *aor, int minStage) {
    return ao_wait_for_stage(aor->ao, minStage);
}

AOResult aor_wait_for_stage_timed(ActiveOpRef *aor, int minStage, uint64_t timeoutMS) {
    return ao_wait_for_stage_timed(aor->ao, minStage, timeoutMS);
}

