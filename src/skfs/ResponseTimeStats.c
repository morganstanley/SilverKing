// ResponseTimeStats.c

/////////////
// includes

#include "ResponseTimeStats.h"
#include "Util.h"

#include <stdlib.h>


////////////////////
// private defines

#define MAX_REASONABLE_RT_MILLIS    (24 * 60 * 60 * 1000)


///////////////////
// implementation

ResponseTimeStats *rts_new(double alpha, uint64_t initialResponseTimeMillis) {
    ResponseTimeStats    *rts;

    rts = (ResponseTimeStats *)mem_alloc(1, sizeof(ResponseTimeStats));
    rts->alpha = alpha;
    rts->oneMinusAlpha = 1.0 - alpha;
    rts->rtAverageMillis = (double)initialResponseTimeMillis;
    rts->rtDevMillis = 0.0;
    pthread_rwlock_init(&rts->rwLock, NULL);
    return rts;
}

void rts_delete(ResponseTimeStats **rts) {
    if (rts != NULL && *rts != NULL) {
        pthread_rwlock_destroy(&(*rts)->rwLock);
        mem_free((void **)rts);
    } else {
        fatalError("bad ptr passed to rts_delete");
    }
}

uint64_t rts_get_rt_average_millis(ResponseTimeStats *rts) {
    uint64_t    result;

    pthread_rwlock_rdlock(&rts->rwLock);
    result = (uint64_t)rts->rtAverageMillis;
    pthread_rwlock_unlock(&rts->rwLock);
    return result;
}

uint64_t rts_get_rt_dev_millis(ResponseTimeStats *rts) {
    uint64_t    result;

    pthread_rwlock_rdlock(&rts->rwLock);
    result = (uint64_t)rts->rtDevMillis;
    pthread_rwlock_unlock(&rts->rwLock);
    return result;
}

void rts_add_sample(ResponseTimeStats *rts, uint64_t responseTimeMillis, int numSamples) {
    double    rt;
    double    error;
    double    oneMinusBeta;
    double    beta;

    srfsLog(LOG_FINE, "rts_add_sample %lu %d", responseTimeMillis, numSamples);
    if (responseTimeMillis < MAX_REASONABLE_RT_MILLIS && numSamples > 0) {
        pthread_rwlock_wrlock(&rts->rwLock);
        oneMinusBeta = pow(rts->oneMinusAlpha, (double)numSamples);
        beta = 1.0 - oneMinusBeta;
        rt = (double)responseTimeMillis / (double)numSamples;
        rts->rtAverageMillis = rts->rtAverageMillis * oneMinusBeta + rt * beta;
        error = rt - rts->rtAverageMillis;
        //rts->rtDevMillis = rts->rtDevMillis * oneMinusBeta + beta * fabs(error);
        rts->rtDevMillis = rts->rtDevMillis + beta * (fabs(error) - rts->rtDevMillis);
        pthread_rwlock_unlock(&rts->rwLock);
    } else {
        srfsLog(LOG_FINE, "rts_add_sample ignoring unreasonable %lu %d", responseTimeMillis, numSamples);
    }
}

void rts_display(ResponseTimeStats *rts) {
    pthread_rwlock_rdlock(&rts->rwLock);
    srfsLog(LOG_WARNING, "%lu %lu", (uint64_t)rts->rtAverageMillis, (uint64_t)rts->rtDevMillis);
    pthread_rwlock_unlock(&rts->rwLock);
}

