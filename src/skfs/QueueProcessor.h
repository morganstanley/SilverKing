// QueueProcessor.h

#ifndef _QUEUE_PROCESSOR_H_
#define _QUEUE_PROCESSOR_H_

/////////////
// includes

#include <pthread.h>

#include "ArrayBlockingQueue.h"


//////////
// types


typedef struct QueueProcessor {
    ArrayBlockingQueue    *abq;
    int            numThreads;
    int            isBatchProcessor;
    int            batchLimit;
    pthread_t    *threads;
    pthread_mutex_t    mutexInstance;
    pthread_mutex_t    *mutex;
    int            nextThreadIndex;
    int    running;
    void (*processElement)(void *, int);
    void (*processBatch)(void **, int, int);
} QueueProcessor;


//////////////////////
// public prototypes

QueueProcessor *qp_new_batch_processor(void (*processBatch)(void **, int, int), char *file, int line, int queueSize, ABQFullMode qFullMode, int numThreads, int batchLimit);
QueueProcessor *qp_new(void (*processElement)(void *, int), char *file, int line, int queueSize, ABQFullMode qFullMode = ABQ_FULL_BLOCK, int numThreads = 1, int batchLimit = 1);
void qp_delete(QueueProcessor **qp);
int qp_add(QueueProcessor *qp, void *item);

#endif
