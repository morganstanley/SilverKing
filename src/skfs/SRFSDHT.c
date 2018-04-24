// SRFSDHT.C

/////////////
// includes

#include "SRFSConstants.h"
#include "SRFSDHT.h"
#include "Util.h"

#include <errno.h>
#include <pthread.h>

////////////////////
// private globals

#define SRFS_TEST
#ifndef SRFS_TEST
static int	_healthCheckMasterIntervalMillis = 15 * 1000;
static int	_healthCheckWorkerIntervalMillis = 5 * 60 * 1000;
static uint64_t	_healthCheckTimeoutMillis = 1 * 60 * 1000;
#else
static int	_healthCheckMasterIntervalMillis = 1 * 1000;
static int	_healthCheckWorkerIntervalMillis = 4 * 1000;
static uint64_t	_healthCheckTimeoutMillis = 20 * 1000;
#endif
static char *_dhtHealthCheckNS = "dht.health";
static const char *_dhtHealthCheckKey = "dht.health.key";


///////////////////////
// private prototypes

static void *sd_health_run_master(void *_sd);
static void *sd_health_run_worker(void *_sd);
static void sd_health_check_master(SRFSDHT *sd);
static void sd_health_check_worker(SRFSDHT *sd);
static void sd_check_dht_health(SRFSDHT *sd);

SKClient  * pClient = NULL;
SKSession * pGlobalSession = NULL;
SKChecksumType::SKChecksumType defaultChecksum = SKChecksumType::NONE;
SKCompression::SKCompression defaultCompression = SKCompression::NONE;
volatile bool exitSignalReceived = FALSE;

///////////////////
// implementation

SRFSDHT *sd_new(char *host, char *gcname, char *zk, SKCompression::SKCompression compression,
				uint64_t minOpTimeout, uint64_t	maxOpTimeout, 
				double devWeight, double dhtWeight) {
	SRFSDHT *sd;

	sd = (SRFSDHT *)mem_alloc(1, sizeof(SRFSDHT));
	sd->status = SD_Enabled;
	sd->host = host;
	sd->gcname = gcname;
	sd->zk = zk;
	sd->compression = compression;
	if (minOpTimeout > maxOpTimeout) {
		fatalError("minOpTimeout > maxOpTimeout", __FILE__, __LINE__);
	}
	sd->minOpTimeout = minOpTimeout;
	sd->maxOpTimeout = maxOpTimeout;
	sd->devWeight = devWeight;
	if (dhtWeight < 0.0 || dhtWeight > 1.0) {
		fatalError("dhtWeight < 0.0 || dhtWeight > 1.0", __FILE__, __LINE__);
	}
	sd->dhtWeight = dhtWeight;
	sd->nfsWeight = 1.0 - dhtWeight;

	mutex_init(&sd->mutexInstance, &sd->mutex);
	cv_init(&sd->cvInstance, &sd->cv);
	sd->running = TRUE;
    sd->ansp = NULL;
	sd->sdSessionMode = SD_SM_Dedicated;
	pthread_create(&sd->healthMasterThread, NULL, sd_health_run_master, sd);
	pthread_create(&sd->healthWorkerThread, NULL, sd_health_run_worker, sd);

	return sd;
}

void sd_delete(SRFSDHT **sd) {
	srfsLog(LOG_FINE, "in sd_delete %llx", sd);
	if (sd != NULL && *sd != NULL) {
		pthread_mutex_lock((*sd)->mutex);
		(*sd)->running = FALSE;
		pthread_cond_broadcast((*sd)->cv);
		pthread_mutex_unlock((*sd)->mutex);
		srfsLog(LOG_FINE, "waiting for sd->healthThread %llx", sd);
		pthread_join((*sd)->healthMasterThread, NULL);
		pthread_join((*sd)->healthWorkerThread, NULL);
		mutex_destroy(&(*sd)->mutex);
		cv_destroy(&(*sd)->cv);
		mem_free((void **)sd);
	} else {
		fatalError("bad ptr in sd_delete");
	}
	srfsLog(LOG_FINE, "out sd_delete");
}

void sd_op_failed(SRFSDHT *sd, SKOperationState::SKOperationState errorCode, char *file, int line) {
	srfsLog(LOG_WARNING, "sd_op_failed %llx %d %s %d", sd, errorCode, file, line);
	sd->status = SD_Unhealthy;
}

int sd_is_enabled(SRFSDHT *sd) {
	srfsLog(LOG_FINE, "sd_is_enabled %llx %d", sd, (sd->status == SD_Enabled));
	return sd->status == SD_Enabled;
}

uint64_t sd_get_dht_timeout(SRFSDHT *sd, ResponseTimeStats *rtsDHT, ResponseTimeStats *rtsNFS, int numOps) {
	double		dhtTimeout;
	double		nfsTimeout;
	uint64_t	timeout;

	dhtTimeout = (double)rts_get_rt_average_millis(rtsDHT) + (double)rts_get_rt_dev_millis(rtsDHT) * sd->devWeight;
	nfsTimeout = (double)rts_get_rt_average_millis(rtsNFS) + (double)rts_get_rt_dev_millis(rtsNFS) * sd->devWeight;
	if (dhtTimeout < nfsTimeout) {
		timeout = (uint64_t)dhtTimeout;
	} else {
		timeout = (uint64_t)nfsTimeout;
	}
	//timeout = (uint64_t)(dhtTimeout * sd->dhtWeight + nfsTimeout * sd->nfsWeight);
	if (timeout < sd->minOpTimeout) {
		timeout = sd->minOpTimeout;
	} else if (timeout > sd->maxOpTimeout) {
		timeout = sd->maxOpTimeout;
	}
	srfsLog(LOG_FINE, "dhtTimeout %5.0f\tnfsTimeout %5.0f\ttimeout%5u", dhtTimeout, nfsTimeout, timeout);
	return timeout * numOps;
}

// health checker

static void *sd_health_run_worker(void *_sd) {
	srfsLog(LOG_FINE, "sd_health_run_worker");
	SKClient::attach(true);
	sd_health_check_worker((SRFSDHT *)_sd);
	SKClient::detach();
	return NULL;
}

static void *sd_health_run_master(void *_sd) {
	srfsLog(LOG_FINE, "sd_health_run_master");
	sd_health_check_master((SRFSDHT *)_sd);
	return NULL;
}

static void sd_health_check_master(SRFSDHT *sd) {
	srfsLog(LOG_FINE, "sd_health_check_master");
	while (sd->running) {
		uint64_t	curTime;

		pthread_mutex_lock(sd->mutex);
		cv_wait_rel(sd->mutex, sd->cv, _healthCheckMasterIntervalMillis);
		if (sd->status == SD_Enabled) {
			curTime = curTimeMillis();
			if (curTime > sd->lastHealthCheckCompletionMillis 
					&& (curTime - sd->lastHealthCheckCompletionMillis > _healthCheckTimeoutMillis)) {
				srfsLog(LOG_WARNING, "Health check taking too long. Marking unhealthy.");
				sd->status = SD_Unhealthy;
			}
		}
		pthread_mutex_unlock(sd->mutex);
		if(exitSignalReceived) { 
			sd->running = FALSE;
		}

	}
	srfsLog(LOG_FINE, "sd_health_check_master terminating");
}

static void sd_health_check_worker(SRFSDHT *sd) {
	srfsLog(LOG_FINE, "sd_health_check_worker");
	try {
		sd->ansp = pGlobalSession->openAsyncNamespacePerspective(_dhtHealthCheckNS);
	} catch(SKClientException & ex){
		srfsLog(LOG_ERROR, "sd_health_check_worker exception opening namespace %s: what: %s\n", _dhtHealthCheckNS, ex.what());
		fatalError("exception in sd_health_check_worker", __FILE__, __LINE__ );
	}
	while (sd->running) {
		pthread_mutex_lock(sd->mutex);
		cv_wait_rel(sd->mutex, sd->cv, _healthCheckWorkerIntervalMillis);
		pthread_mutex_unlock(sd->mutex);
		sd_check_dht_health(sd);
		if(exitSignalReceived) { 
			sd->running = FALSE;
		}
	}
	srfsLog(LOG_FINE, "sd_health_check_worker terminating");
	sd->ansp->waitForActiveOps();
	sd->ansp->close();
    delete sd->ansp;
    sd->ansp = NULL;
}

static void sd_check_dht_health(SRFSDHT *sd) {
    StrVector healthKey;
    healthKey.push_back(string(_dhtHealthCheckKey));
	SKAsyncValueRetrieval * pValRetrieval = NULL;
	SKOperationState::SKOperationState opState = SKOperationState::INCOMPLETE;
	SKFailureCause::SKFailureCause failReason = SKFailureCause::TIMEOUT;
	try {
		pValRetrieval  = sd->ansp->get(&healthKey);
		pValRetrieval->waitForCompletion();
		opState = pValRetrieval->getOperationState(_dhtHealthCheckKey);
		try {
			if (opState == SKOperationState::FAILED) 
				failReason = pValRetrieval->getFailureCause(); // it is invalid if op is not FAILED 
		} catch (...) {srfsLog(LOG_WARNING, "Health check failed to query FailureCause");}
	} catch ( ... ) {
		srfsLog(LOG_WARNING, "exception caught in sd_check_dht_health");
	}
	if(pValRetrieval) {
		pValRetrieval->close();
		delete pValRetrieval;
	}
	
	pthread_mutex_lock(sd->mutex);
	sd->lastHealthCheckCompletionMillis = curTimeMillis();
	if(opState == SKOperationState::SUCCEEDED || ( opState == SKOperationState::FAILED && failReason == SKFailureCause::NO_SUCH_VALUE ) ) {
		if (sd->status == SD_Enabled) {
			srfsLog(LOG_WARNING, "Health check succeeded. Already enabled.");
		} else {
			srfsLog(LOG_WARNING, "Health check succeeded. Enabling");
			sd->status = SD_Enabled;
		}
	} else {
		srfsLog(LOG_WARNING, "Health check failed");
		sd->status = SD_Unhealthy;
	}
	pthread_mutex_unlock(sd->mutex);
}

SKSession *sd_new_session(SRFSDHT *sd) {
	if (sd->sdSessionMode == SD_SM_Dedicated) {
		SKSession	*pSession;
		
		pSession = pClient->openSession(sessOption);
		if (!pSession) {
			fatalError("Failed to create newSession()", __FILE__, __LINE__ );
		}
		return pSession;
	} else {
		return pGlobalSession;
	}
}

uint64_t sd_parse_timeout(const char *s, uint64_t _default) {
    uint64_t    t;
    
    t = strtoull(s, NULL, 10);
    if (t == 0) {
        if (errno < 0) {
            return _default;
        } else {
            return 0;
        }
    } else {
        return t;
    }
}
