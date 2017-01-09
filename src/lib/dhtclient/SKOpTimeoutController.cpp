#include "SKOpTimeoutController.h"
#include "SKAsyncOperation.h"

#include "jace/proxy/com/ms/silverking/cloud/dht/client/OpTimeoutController.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::OpTimeoutController;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncOperation.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncOperation;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;

int SKOpTimeoutController::getMaxAttempts(SKAsyncOperation * op) {
	int  v = (int)((OpTimeoutController*)pImpl)->getMaxAttempts(*(AsyncOperation*)op->getPImpl()); 
	return v;
}

int SKOpTimeoutController::getRelativeTimeoutMillisForAttempt(SKAsyncOperation * op, int attemptIndex) {
	int  v = (int)((OpTimeoutController*)pImpl)->getRelativeTimeoutMillisForAttempt(*(AsyncOperation*)op->getPImpl(), attemptIndex); 
	return v;
}

int SKOpTimeoutController::getMaxRelativeTimeoutMillis(SKAsyncOperation *op) {
	int  v = (int)((OpTimeoutController*)pImpl)->getMaxRelativeTimeoutMillis(*(AsyncOperation*)op->getPImpl()); 
	return v;
}

std::string SKOpTimeoutController::toString() {
	std::string s = (std::string)(((OpTimeoutController*)pImpl)->toString());
	return s;
}

OpTimeoutController * SKOpTimeoutController::getPImpl() {
    return (OpTimeoutController *)pImpl;
}

////////

SKOpTimeoutController::SKOpTimeoutController(void *opTimeoutController)
{
    pImpl = opTimeoutController;
}

SKOpTimeoutController::SKOpTimeoutController()
{
    pImpl = NULL;
}

SKOpTimeoutController::~SKOpTimeoutController() 
{ 
	if (pImpl) {
		OpTimeoutController* pOpTimeoutController = (OpTimeoutController*)pImpl;
		delete pOpTimeoutController;
		pImpl = NULL;
	}
}



