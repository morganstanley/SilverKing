// ActiveOp.c

/////////////
// includes

#include "ActiveOp.h"
#include "Util.h"

#include <string.h>


///////////////////////
// private prototypes

static int _ao_find_empty_ref(ActiveOp *ao) ;
static void ao_check_for_deletion(ActiveOp *ao);


///////////////////
// implementation

ActiveOp *ao_new(void *target, void (*delete_function)(void **)) {
	ActiveOp	*ao;
	int			i;

	ao = (ActiveOp *)mem_alloc(1, sizeof(ActiveOp));
	ao->target = target;
	ao->stage = 0;
	ao->delete_function = delete_function;
	//for (i = 0; i < AO_MAX_REFS; i++) { // mem_alloc zeros out and AOR_Invalid is zero so not needed
	//	ao->refStatus[i] = AOR_Invalid;
	//}
	mutex_init(&ao->mutexInstance, &ao->mutex);
	cv_init(&ao->cvInstance, &ao->cv);
	srfsLog(LOG_FINE, "out ao_new %llx", ao);
	return ao;
}

void ao_delete(ActiveOp **ao) {
	srfsLog(LOG_FINE, "in ao_delete %llx", ao);
	if (ao != NULL && *ao != NULL) {
		mutex_destroy(&(*ao)->mutex);
		cv_destroy(&(*ao)->cv);
		(*ao)->delete_function(&(*ao)->target);
        if ((*ao)->target != NULL) {
            mem_free(&(*ao)->target);
        }
        if ((*ao)->rValLength != 0) {
            if ((*ao)->rVal != NULL) {
                mem_free(&(*ao)->rVal);
            }
        }
		mem_free((void **)ao);
	} else {
		fatalError("bad ptr passed to ao_delete");
	}
	srfsLog(LOG_FINE, "out ao_delete %llx", ao);
}

void *ao_get_target(ActiveOp *ao) {
	return ao->target;
}

void *ao_get_rVal(ActiveOp *ao) {
	return ao->rVal;
}

size_t ao_get_rValLength(ActiveOp *ao) {
	return ao->rValLength;
}

int ao_create_ref(ActiveOp *ao) {
	int	ref;

	ref = -1;
	pthread_mutex_lock(ao->mutex);
    
    if (ao->nextRef >= AO_RECYCLE_THRESHOLD) {
        ref = _ao_find_empty_ref(ao);
    }
    if (ref >= 0) {
        ao->refStatus[ref] = AOR_Created;
    } else {
        if (ao->nextRef < AO_MAX_REFS) {
            ref = ao->nextRef++;
            if (ao->refStatus[ref] != AOR_Invalid) {
                fatalError("ao->refStatus[ref] != AOR_Invalid", __FILE__, __LINE__);
            }
            ao->refStatus[ref] = AOR_Created;
        } else {
            fatalError("AO_MAX_REFS exceeded", __FILE__, __LINE__);
        }
    }
    
	pthread_mutex_unlock(ao->mutex);
	return ref;
}

// lock must be held
static int _ao_find_empty_ref(ActiveOp *ao) {
	int	i;

    for (i = 0; i < ao->nextRef; i++) {
        if (ao->refStatus[i] != AOR_Created) {
            return i;
        }
    }
    return -1;
}

static void ao_check_for_deletion(ActiveOp *ao) {
	int	doDelete;
	int	i;

	doDelete = TRUE;
	//pthread_mutex_lock(ao->mutex); - this must already be held
	if (!ao->toDelete) {
		for (i = 0; i < ao->nextRef; i++) {
			if (ao->refStatus[i] != AOR_Destroyed) {
				doDelete = FALSE;
				break;
			}
		}
		if (doDelete) {
			ao->toDelete = TRUE;
		}
	} else {
		doDelete = FALSE;
	}
	pthread_mutex_unlock(ao->mutex);
	if (doDelete) {
		ActiveOp	*oldAO;

		srfsLog(LOG_FINE, "All ActiveOp references destroyed. Deleting %llx", ao);
		oldAO = ao;
		ao_delete(&ao);
	}
}

void ao_delete_ref(ActiveOp *ao, int ref) {
	pthread_mutex_lock(ao->mutex);
	if (ref >= ao->nextRef) {
		fatalError("ref >= ao->nextRef", __FILE__, __LINE__);
	}
	if (ao->refStatus[ref] != AOR_Created) {
		fatalError("ao->refStatus[ref] != AOR_Created", __FILE__, __LINE__);
	}
	ao->refStatus[ref] = AOR_Destroyed;
	//pthread_mutex_unlock(ao->mutex); - this is performed in check for deletion
	ao_check_for_deletion(ao);
}

AOResult ao_wait_for_stage(ActiveOp *ao, int minStage) {
    AOResult    result;
    
	pthread_mutex_lock(ao->mutex);
	while (ao->stage < minStage && ao->stage != AO_STAGE_COMPLETE) {
		pthread_cond_wait(ao->cv, ao->mutex);
	}
    result = ao->result;
	pthread_mutex_unlock(ao->mutex);
    return result;
}

AOResult ao_wait_for_stage_timed(ActiveOp *ao, int minStage, uint64_t timeoutMS) {
    AOResult    result;
	uint64_t	deadline;

	deadline = curTimeMillis() + timeoutMS;
	pthread_mutex_lock(ao->mutex);
	while (ao->stage < minStage && ao->stage != AO_STAGE_COMPLETE && curTimeMillis() < deadline) {
		cv_wait_abs(ao->mutex, ao->cv, deadline);
	}
	if (ao->stage < minStage && ao->stage != AO_STAGE_COMPLETE) {
        result = AOResult_Timeout;
    } else {
        result = ao->result;
    }
	pthread_mutex_unlock(ao->mutex);
    return result;
}

void ao_set_stage(ActiveOp *ao, int stage) {
	pthread_mutex_lock(ao->mutex);
	if (stage > ao->stage) {
		ao->stage = stage;
        if (ao->result != AOResult_Incomplete) {
            srfsLog(LOG_ERROR, "Unexpected ao->result != AOResult_Incomplete in ao_set_stage");
        }
		pthread_cond_broadcast(ao->cv);
    }
	pthread_mutex_unlock(ao->mutex);
}

AOResult ao_wait_for_completion(ActiveOp *ao) {
	return ao_wait_for_stage(ao, AO_STAGE_COMPLETE);
}

void ao_set_complete(ActiveOp *ao, AOResult result, void *rVal, size_t rValLength) {
    if (result == AOResult_Incomplete) {
        srfsLog(LOG_ERROR, "Unexpected result == AOResult_Incomplete in ao_set_complete");
    } else {
        pthread_mutex_lock(ao->mutex);
        ao->stage = AO_STAGE_COMPLETE;
        if (ao->result == AOResult_Incomplete) {
            ao->result = result;
            if (rVal != NULL) {
                if (ao->rVal != NULL) {
                    fatalError("Result incomplete, but rVal already set", __FILE__, __LINE__);
                }
                if (rValLength != 0) {
                    ao->rVal = mem_dup(rVal, rValLength);
                } else {
                    ao->rVal = rVal;
                }
                ao->rValLength = rValLength;
            }
            pthread_cond_broadcast(ao->cv);
        } else {
            srfsLog(LOG_INFO, "Ignoring result for already complete op");
        }
        pthread_mutex_unlock(ao->mutex);
    }
}

void ao_set_complete_error(ActiveOp *ao, int errorCode) {
    ao_set_complete(ao, AOResult_Error, (void *)(uint64_t)errorCode, 0);
}
