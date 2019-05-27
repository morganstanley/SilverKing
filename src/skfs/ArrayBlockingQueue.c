// ArrayBlockingQueue.c

/////////////
// includes

#include <stdlib.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>

#include "ArrayBlockingQueue.h"
#include "Util.h"


///////////////////
// implementation

/**
 * Create a new ABQ instance.
 */
ArrayBlockingQueue *abq_new(int size, ABQFullMode qFullMode) {
    ArrayBlockingQueue    *abq;

    if (size <= 0) {
        fatalError("Invalid queue size");
    }
    abq = (ArrayBlockingQueue *)mem_alloc(1, sizeof(ArrayBlockingQueue));
    abq->size = size;
    abq->qFullMode = qFullMode;
    srfsLog(LOG_FINE, "new abq size %d qFullMode", abq->size, abq->qFullMode);
    abq->entries = (BQEntry *)mem_alloc(size, sizeof(BQEntry));
    abq->head = 0;
    abq->tail = 0;
    mutex_init(&abq->mutexInstance, &abq->mutex);
    cv_init(&abq->emptyCVInstance, &abq->emptyCV);
    cv_init(&abq->fullCVInstance, &abq->fullCV);
    return abq;
}

/**
 * Delete an ABQ instance.
 */
void abq_delete(ArrayBlockingQueue **abq) {
    if (abq != NULL && *abq != NULL) {
        mutex_destroy(&(*abq)->mutex);
        cv_destroy(&(*abq)->emptyCV);
        cv_destroy(&(*abq)->fullCV);
        mem_free((void **) &((*abq)->entries) );
        mem_free((void **)abq);
    } else {
        fatalError("bad ptr passed to abq_delete");
    }
}

static int _abq_is_full(ArrayBlockingQueue *abq) {
    int    modResult;

    modResult = (abq->head - 1) % abq->size;
    if (modResult < 0) {
        // compilers can do whatever they want per standard; fix it
        modResult += abq->size; 
    }
    return modResult == abq->tail;
}

static int _abq_is_empty(ArrayBlockingQueue *abq) {
    return abq->head == abq->tail;
}

/**
 * Enqueue an entry. Block if no room.
 */
int abq_put(ArrayBlockingQueue *abq, void *data) {
    int added;
    
    pthread_mutex_lock(abq->mutex);
    //srfsLog(LOG_FINE, "abq_put %llx head %d tail %d data %llx", abq, abq->head, abq->tail, data);
    if (abq->qFullMode == ABQ_FULL_DROP && _abq_is_full(abq)) {
        srfsLog(LOG_FINE, "abq full dropping %llx", data);
        added = FALSE;
    } else {
        while (_abq_is_full(abq)) {
            //srfsLog(LOG_FINE, "abq full waiting %llx", abq);
            pthread_cond_wait(abq->fullCV, abq->mutex);
        }
        abq->entries[abq->tail].data = data;
        abq->tail = (abq->tail + 1) % abq->size;
        //srfsLog(LOG_FINE, "signal %llx", abq->emptyCV);
        pthread_cond_signal(abq->emptyCV);
        added = TRUE;
    }
    pthread_mutex_unlock(abq->mutex);
    return added;
}

/**
 * Dequeue an entry. Block until one can be obtained.
 */
void *abq_take(ArrayBlockingQueue *abq) {
    void    *data;

    pthread_mutex_lock(abq->mutex);
    while (_abq_is_empty(abq)) {
        //srfsLog(LOG_FINE, "abq empty waiting %llx %llx", abq, abq->emptyCV);
        pthread_cond_wait(abq->emptyCV, abq->mutex);
    }
    data = abq->entries[abq->head].data;
    srfsLog(LOG_FINE, "abq_take head %d tail %d data %llx", abq->head, abq->tail, data);
    abq->head = (abq->head + 1) % abq->size;
    pthread_cond_signal(abq->fullCV);
    pthread_mutex_unlock(abq->mutex);
    return data;
}

/**
 * Dequeue multiple entries. Block until at least one can be obtained.
 */
int abq_take_multi(ArrayBlockingQueue *abq, void **batch, int batchLimit) {
    int        batchSize;

    if (batchLimit <= 0) {
        fatalError("batchLimit <= 0", __FILE__, __LINE__);
    }
    pthread_mutex_lock(abq->mutex);
    while (_abq_is_empty(abq)) {
        //srfsLog(LOG_FINE, "abq empty waiting %llx %llx %d %d", abq, abq->emptyCV, abq->head, abq->tail);
        //srfsLog(LOG_FINE, "abq cond_wait %llx %llx %d %d", abq->emptyCV, abq->mutex, pthread_self(), syscall(SYS_gettid));
        pthread_cond_wait(abq->emptyCV, abq->mutex);
        //srfsLog(LOG_FINE, "back abq cond_wait %llx %llx", abq->emptyCV, abq->mutex);
    }
    //srfsLog(LOG_FINE, "abq_take_multi out of wait loop %llx %llx %d %d", abq, abq->emptyCV, abq->head, abq->tail);
    batchSize = 0;
    while (!_abq_is_empty(abq) && batchSize < batchLimit) {
        batch[batchSize] = abq->entries[abq->head].data;
        //srfsLog(LOG_FINE, "abq_take_multi head %d tail %d batch[batchSize] %llx", abq, abq->head, abq->tail, batch[batchSize]);
        batchSize++;
        abq->head = (abq->head + 1) % abq->size;
        if(!batch[batchSize-1]) //stop on null-entries sent to QP upon graceful exit notification
            break ;
    }
    pthread_cond_signal(abq->fullCV);
    pthread_mutex_unlock(abq->mutex);
    //srfsLog(LOG_FINE, "abq_take_multi %llx %llx returning %d", abq, abq->fullCV, batchSize);
    return batchSize;
}

