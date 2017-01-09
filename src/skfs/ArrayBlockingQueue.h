// ArrayBlockingQueue.h

#ifndef _ARRAY_BLOCKING_QUEUE_H_
#define _ARRAY_BLOCKING_QUEUE_H_

/////////////
// includes

#include <pthread.h>


//////////
// types

typedef enum {ABQ_FULL_BLOCK, ABQ_FULL_DROP} ABQFullMode;

/** private type used to store entries */
typedef struct BQEntry {
	void *data;
} BQEntry;

typedef struct ArrayBlockingQueue {
	BQEntry	*entries;
	int		size;
	ABQFullMode	qFullMode;
	int		head;
	int		tail;
	pthread_mutex_t	mutexInstance;
	pthread_mutex_t	*mutex;
	pthread_cond_t	emptyCVInstance;
	pthread_cond_t	*emptyCV;
	pthread_cond_t	fullCVInstance;
	pthread_cond_t	*fullCV;
} ArrayBlockingQueue;


//////////////////////
// public prototypes

ArrayBlockingQueue *abq_new(int size, ABQFullMode qFullMode = ABQ_FULL_BLOCK);
void abq_delete(ArrayBlockingQueue **abq);
int abq_put(ArrayBlockingQueue *abq, void *data);
void *abq_take(ArrayBlockingQueue *abq);
int abq_take_multi(ArrayBlockingQueue *abq, void **batch, int batchLimit);
//void abq_peek(ArrayBlockingQueue *abq);

#endif
