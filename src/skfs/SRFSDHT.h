// SRFSDHT.h

#ifndef _SRFS_DHT_H_
#define _SRFS_DHT_H_

/////////////
// includes


#include "skbasictypes.h"
//#include "skcontainers.cpp"
#include "SKClient.h"
#include "SKSession.h"
#include "SKAsyncNSPerspective.h"
#include "SKSyncNSPerspective.h"
#include "SKStoredValue.h"

#include "skcontainers.h"
#include "SKValueCreator.h"
#include "SKGridConfiguration.h"
#include "SKClientDHTConfiguration.h"
#include "SKSessionOptions.h"
#include "SKNamespacePerspectiveOptions.h"
#include "SKPutOptions.h"
#include "SKGetOptions.h"
#include "SKWaitOptions.h"
#include "SKNamespaceOptions.h"
#include "SKAsyncPut.h"
#include "SKAsyncRetrieval.h"
#include "SKAsyncValueRetrieval.h"
#include "SKAsyncSingleValueRetrieval.h"
#include "SKAsyncSyncRequest.h"
#include "SKAsyncSnapshot.h"

#include "SKRetrievalException.h"
#include "SKPutException.h"
#include "SKKeyedOperationException.h"
#include "SKSyncRequestException.h"
#include "SKSnapshotException.h"
#include "SKWaitForCompletionException.h"
#include "SKNamespaceCreationException.h"
#include "SKOperationException.h"
#include "SKClientException.h"

#include "ResponseTimeStats.h"
#include <stdint.h>


extern SKClient  * pClient;
extern SKSession * pGlobalSession;
extern SKNamespaceOptions * pNSOptions ;
extern SKNamespacePerspectiveOptions * pNspOptions;
extern SKNamespacePerspectiveOptions * pAttrNspOptions;
extern SKNamespacePerspectiveOptions * pDirNspOptions;
extern SKPutOptions * pPutOptions;
extern SKChecksumType::SKChecksumType defaultChecksum ;
extern SKCompression::SKCompression defaultCompression ;
extern const char * _namespaceOptions ;
extern const char * _putOptions ;
extern volatile bool exitSignalReceived;
extern SKSessionOptions *sessOption;

//////////
// types

typedef SKVector<const std::string*>     KeyVector;
typedef SKVector<std::string> 			 StrVector;
typedef SKMap<std::string, std::string>	 StrStrMap;
typedef SKMap<std::string, SKVal*>	     StrValMap;
typedef SKMap<std::string, SKStoredValue*> StrSVMap;
typedef SKMap<std::string, SKOperationState::SKOperationState> OpStateMap;

typedef enum {SD_Disabled, SD_Enabled, SD_Unhealthy} SDStatus;
typedef enum {SD_SM_Dedicated, SD_SM_Global} SD_SessionMode;

typedef struct SRFSDHT {
	SDStatus	status;
	uint64_t	lastHealthCheckCompletionMillis;
	char		*host;
	//int			port;
	//char		*name;
	char		*gcname;
	char		*zk;
	uint64_t	minOpTimeout;
	uint64_t	maxOpTimeout;
	double		devWeight;
	double		dhtWeight;
	double		nfsWeight;
	pthread_mutex_t	mutexInstance;
	pthread_mutex_t	*mutex;
	pthread_cond_t	cvInstance;
	pthread_cond_t	*cv;
	pthread_t	healthMasterThread;
	pthread_t	healthWorkerThread;
	int			running;
	SKCompression::SKCompression	compression;
    SKAsyncNSPerspective * ansp;
	SD_SessionMode	sdSessionMode;
} SRFSDHT;

///////////////
// prototypes

SRFSDHT *sd_new(char *host, char *gcname, char *zk, SKCompression::SKCompression compression,
				uint64_t minOpTimeout, uint64_t	maxOpTimeout, 
				double devWeight, double dhtWeight);
void sd_delete(SRFSDHT **sd);
void sd_op_failed(SRFSDHT *sd, SKOperationState::SKOperationState errorCode, char *file = NULL, int line = 0);
int sd_is_enabled(SRFSDHT *sd);
SKSession *sd_new_session(SRFSDHT *sd);
uint64_t sd_get_dht_timeout(SRFSDHT *sd, ResponseTimeStats *rtsDHT, ResponseTimeStats *rtsNFS, int numOps);
uint64_t sd_parse_timeout(const char *s, uint64_t _default);

#endif
