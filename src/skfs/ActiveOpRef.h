// ActiveOpRef.h

#ifndef _ACTIVE_OP_REF_H_
#define _ACTIVE_OP_REF_H_

/////////////
// includes

#include "ActiveOp.h"


//////////
// types

typedef struct ActiveOpRef {
    ActiveOp    *ao;
    int            ref;
    int            deleted;
    char        *file;
    int            line;
} ActiveOpRef;

ActiveOpRef *aor_new(ActiveOp *ao, char *file, int line);
void aor_delete(ActiveOpRef **aor);
void *aor_get_target(ActiveOpRef *aor);
void *aor_get_rVal(ActiveOpRef *aor);
size_t aor_get_rValLength(ActiveOpRef *aor);
AOResult aor_wait_for_completion(ActiveOpRef *aor);
AOResult aor_wait_for_stage(ActiveOpRef *aor, int minStage);
AOResult aor_wait_for_stage_timed(ActiveOpRef *aor, int minStage, uint64_t timeoutMS);

#endif
