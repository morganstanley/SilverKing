// SKHealthMonitor.h

#ifndef _SK_HEALTH_MONITOR_H_
#define _SK_HEALTH_MONITOR_H_

/////////////
// includes

#include "SRFSDHT.h"

#include <pthread.h>


//////////
// types

typedef enum {SHM_RH_Healthy = 0, SHM_RH_Unhealthy, SHM_RH_Offline} SHM_RingHealth;

typedef struct SKHealthMonitor {
    SKSyncNSPerspective     *systemNSP;
    SHM_RingHealth          ringHealth;
	pthread_t	            threadInstance;
	pthread_t	            *thread;    
} SKHealthMonitor;


//////////////////////
// public prototypes

SKHealthMonitor *shm_new(SKSyncNSPerspective *systemNSP);
SHM_RingHealth shm_get_ring_health(SKHealthMonitor *shm);

#endif