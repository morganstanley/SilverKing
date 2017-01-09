#include "SKSyncRequestException.h"
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/SyncRequestException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SyncRequestException;

SKSyncRequestException::SKSyncRequestException(SyncRequestException * pSre, const char * fileName, int lineNum) 
	: SKClientException(pSre, fileName, lineNum) { 	/*pImpl = pSre;*/ }

SKSyncRequestException::~SKSyncRequestException()  throw () { }
