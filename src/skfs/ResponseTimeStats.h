// ResponseTimeStats.h

#ifndef _RESPONSE_TIME_STATS_H_
#define _RESPONSE_TIME_STATS_H_

/////////////
// includes

#include <math.h>
#include <pthread.h>
#include <stdint.h>


//////////
// types

typedef struct ResponseTimeStats {
    double    rtAverageMillis;
    double    rtDevMillis;
    double    alpha;
    double    oneMinusAlpha;
    pthread_rwlock_t    rwLock;
} ResponseTimeStats;


/////////////////////
// prototypes

ResponseTimeStats *rts_new(double alpha, uint64_t initialResponseTimeMillis);
void rts_delete(ResponseTimeStats **rts);
uint64_t rts_get_rt_average_millis(ResponseTimeStats *rts);
uint64_t rts_get_rt_dev_millis(ResponseTimeStats *rts);
void rts_add_sample(ResponseTimeStats *rts, uint64_t responseTimeMillis, int numSamples = 1);
void rts_display(ResponseTimeStats *rts);

#endif
