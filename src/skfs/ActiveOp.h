// ActiveOp.h

#ifndef _ACTIVE_OP_H_
#define _ACTIVE_OP_H_

/////////////
// includes

//#include "FileBlockID.h"

#include <pthread.h>
#include <stdint.h>


////////////
// defines

#define AO_MAX_REFS 512 
#define AO_STAGE_COMPLETE 65536
#define AO_RECYCLE_THRESHOLD    500
// AO_RECYCLE_THRESHOLD must be < AO_MAX_REFS for reference number recycling to be active


//////////
// types

typedef enum {AOR_Invalid = 0, AOR_Created, AOR_Destroyed} AORefStatus;
typedef enum {AOResult_Incomplete = 0, AOResult_Timeout, AOResult_Error, AOResult_Success} AOResult;

typedef struct ActiveOp {
	void			*target;
    void            *rVal;
    size_t          rValLength;
    AOResult        result;
	int				stage;
	AORefStatus		refStatus[AO_MAX_REFS];
	int				nextRef;
	int				toDelete;
	void (*delete_function)(void **);
	pthread_mutex_t	mutexInstance;
	pthread_mutex_t	*mutex;
	pthread_cond_t	cvInstance;
	pthread_cond_t	*cv;
} ActiveOp;


///////////////
// prototypes

ActiveOp *ao_new(void *target, void (*delete_function)(void **));
void ao_delete(ActiveOp **ao);
void *ao_get_target(ActiveOp *ao);
void *ao_get_rVal(ActiveOp *ao);
size_t ao_get_rValLength(ActiveOp *ao);
int ao_create_ref(ActiveOp *ao);
void ao_delete_ref(ActiveOp *ao, int ref);
AOResult ao_wait_for_completion(ActiveOp *ao);
void ao_set_complete(ActiveOp *ao, AOResult result = AOResult_Success, void *rVal = NULL, size_t rValLength = 0);
void ao_set_complete_error(ActiveOp *ao, int errorCode);
AOResult ao_wait_for_stage(ActiveOp *ao, int minStage);
AOResult ao_wait_for_stage_timed(ActiveOp *ao, int minStage, uint64_t timeoutMS);
void ao_set_stage(ActiveOp *ao, int stage);
void ao_set_pinned(ActiveOp *ao, char *pinned);


#endif
