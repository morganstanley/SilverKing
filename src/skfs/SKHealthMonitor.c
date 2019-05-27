// SKHealthMonitor.c

/////////////
// includes

#include "SKHealthMonitor.h"
#include "Util.h"


//////////////////
// local defines

#define SHM_RING_HEALTH_KEY "ringHealth"
#define SHM_RING_UNHEALTHY_VALUE "Unhealthy"
#define SHM_RING_UNHEALTHY_LENGTH 9
#define SHM_RING_OFFLINE_VALUE "Offline"
#define SHM_RING_OFFLINE_LENGTH 7
#define SHM_RING_HEALTH_READ_INTERVAL_MILLIS (5 * 1000)


///////////////////////
// private prototypes

static void *shm_run(void *_shm);


///////////////////
// implementation

SKHealthMonitor *shm_new(SKSyncNSPerspective *systemNSP) {
    SKHealthMonitor *shm;

    shm = (SKHealthMonitor*)mem_alloc(1, sizeof(SKHealthMonitor));
    shm->systemNSP = systemNSP;
    pthread_create(&shm->threadInstance, NULL, shm_run, shm);
    shm->thread = &shm->threadInstance;
    return shm;
}

SHM_RingHealth shm_get_ring_health(SKHealthMonitor *shm) {
    return shm->ringHealth;
}

static SHM_RingHealth shm_read_ring_health(SKHealthMonitor *shm) {
    SKVal       *pval;
    SHM_RingHealth  ringHealth;

    pval = shm->systemNSP->get(SHM_RING_HEALTH_KEY);
    if (pval != NULL) {
        if (pval->m_pVal != NULL) {
            srfsLog(LOG_FINE, "%s %d", pval->m_pVal, pval->m_len);
            if (pval->m_len == SHM_RING_UNHEALTHY_LENGTH && !strncmp((const char *)pval->m_pVal, SHM_RING_UNHEALTHY_VALUE, SHM_RING_UNHEALTHY_LENGTH)){
                ringHealth = SHM_RH_Unhealthy;
            } else {
                if (pval->m_len == SHM_RING_OFFLINE_LENGTH && !strncmp((const char *)pval->m_pVal, SHM_RING_OFFLINE_VALUE, SHM_RING_OFFLINE_LENGTH)){
                    ringHealth = SHM_RH_Offline;
                } else {
                    ringHealth = SHM_RH_Healthy;
                }
            }
        } else {
            ringHealth = SHM_RH_Healthy;
        }
        sk_destroy_val(&pval);
    } else {
        ringHealth = SHM_RH_Healthy;
    }
    return ringHealth;
}

static void *shm_run(void *_shm) {
    SKHealthMonitor *shm;
    
    shm = (SKHealthMonitor *)_shm;
    srfsLog(LOG_WARNING, "shm running");
    while (TRUE) {
        msleep(SHM_RING_HEALTH_READ_INTERVAL_MILLIS);
        shm->ringHealth = shm_read_ring_health(shm);
    }
}
