// QueueProcessor.c

/////////////
// includes 

#include "QueueProcessor.h"
#include "Util.h"
#include "SRFSDHT.h"

#include <sys/prctl.h>
#include <stdlib.h>


///////////////////////
// private prototypes

static void *qp_run(void *_qp);


///////////////////
// implementation

QueueProcessor *qp_new_batch_processor(void (*processBatch)(void **, int, int), char *file, int line, int queueSize, ABQFullMode qFullMode, int numThreads, int batchLimit) {
	QueueProcessor	*qp;

	qp = qp_new((void (*)(void *, int))processBatch, file, line, queueSize, qFullMode, numThreads, batchLimit);
	/*
	qp->isBatchProcessor = TRUE;
	qp->processBatch = (void (*)(void **, int))processBatch;
	qp->processElement = NULL;
	*/
	return qp;
}

QueueProcessor *qp_new(void (*processElement)(void *, int), char *file, int line, int queueSize, ABQFullMode qFullMode, int numThreads, int batchLimit) {
	QueueProcessor *qp;
	int	i;

	qp = (QueueProcessor *)mem_alloc(1, sizeof(QueueProcessor));
	qp->mutex = &qp->mutexInstance;
	if (batchLimit == -1) {
		qp->isBatchProcessor = TRUE;
		batchLimit = 1;
	} else {
		qp->isBatchProcessor = batchLimit > 1;
	}
	qp->abq = abq_new(queueSize, qFullMode);
	qp->running = TRUE;
	qp->numThreads = numThreads;
	qp->batchLimit = batchLimit;
	srfsLog(LOG_FINE, "qp_new batchLimit %d numThreads %d\t%s %d", qp->batchLimit, qp->numThreads, file, line);
	if (batchLimit <= 0) {
		srfsLog(LOG_WARNING, "qp_new detected bogus batchLimit. Using 1.");
		batchLimit = 1;
	}
	if (!qp->isBatchProcessor) {
		qp->processElement = processElement;
		qp->processBatch = NULL;
	} else {
		qp->processBatch = (void (*)(void **, int, int))processElement;
		qp->processElement = NULL;
	}
	qp->threads = (pthread_t *)mem_alloc(numThreads, sizeof(pthread_t));
	for (i = 0; i < qp->numThreads; i++) {
		pthread_create(&qp->threads[i], NULL, qp_run, qp);
	}
	mutex_init(&qp->mutexInstance, &qp->mutex);
	return qp;
}

/**
 * Delete QP instance.
 */
void qp_delete(QueueProcessor **qp) {
	(*qp)->running = FALSE;
	if (qp != NULL && *qp != NULL) {
		int	i;

		for (i = 0; i < (*qp)->numThreads; i++) {
			pthread_join((*qp)->threads[i], NULL);
		}
		mem_free((void **) &((*qp)->threads));
		mutex_destroy(&(*qp)->mutex);
		abq_delete( &((*qp)->abq) );
		mem_free((void **)qp);
	} else {
		fatalError("bad ptr passed to qp_delete");
	}
}

/**
 * Add an item to the work queue.
 */
int qp_add(QueueProcessor *qp, void *item) {
	if (qp->running || !item) {
		//srfsLog(LOG_FINE, "qp_add %llx %llx %llx", qp, qp->abq, item);
		return abq_put(qp->abq, item);
	}
	return FALSE;
}

static void *qp_run(void *_qp) {
	QueueProcessor	*qp;
	void	*data;
	int		curThreadIndex;
	
	//prctl(PR_SET_NAME, "QueueProcessor", 0, 0, 0);
	srfsLog(LOG_FINE, "qp_run start");
	SKClient::attach(true);  //
	qp = (QueueProcessor *)_qp;
	pthread_mutex_lock(qp->mutex);
	curThreadIndex = qp->nextThreadIndex++;
	pthread_mutex_unlock(qp->mutex);
	if (curThreadIndex > qp->numThreads) {
		fatalError("curThreadIndex > qp->numThreads", __FILE__, __LINE__);
	}
	srfsLog(LOG_FINE, "qp_run getting work");
	while (qp->running) {
		void *batch[qp->batchLimit];

		//srfsLog(LOG_FINE, "qp_run batchLimit %d %llx %llx", qp->batchLimit, qp, qp->abq);
		if (!qp->isBatchProcessor) {
			data = abq_take(qp->abq);
			//srfsLog(LOG_FINE, "qp_run calling process element");
			if(data) {
				qp->processElement(data, curThreadIndex);
			} else {
				srfsLog(LOG_FINE, "qp_run null data");
			}
		} else {
			int	batchSize;

			batchSize = abq_take_multi(qp->abq, batch, qp->batchLimit);
			if (!batch[batchSize - 1]) {
				//null element at the end notifies of process completion
				if (batchSize > 1) {
					batchSize--;
					srfsLog(LOG_FINE, "qp_run calling process batch with last null");
					qp->processBatch(batch, batchSize, curThreadIndex);
				} else {
					srfsLog(LOG_FINE, "qp_run empty batch");
				}
			} else {
				//srfsLog(LOG_FINE, "qp_run calling process batch %llx %llx", qp, qp->abq);
				qp->processBatch(batch, batchSize, curThreadIndex);
			}
		}
		if (exitSignalReceived) { 
			qp->running = FALSE;
		}
	}
	SKClient::detach();
	srfsLog(LOG_FINE, "qp_run exit");
	return NULL;
}
