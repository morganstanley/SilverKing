#include "SKSimpleTimeoutController.h"
#include "SKAsyncOperation.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;

#include "jace/proxy/com/ms/silverking/cloud/dht/client/SimpleTimeoutController.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SimpleTimeoutController;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/OpTimeoutController.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::OpTimeoutController;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncOperation.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncOperation;


SKSimpleTimeoutController * SKSimpleTimeoutController::parse(const char * def)
{
	SimpleTimeoutController * pSimpleTimeoutController = new SimpleTimeoutController(java_cast<SimpleTimeoutController>(
			SimpleTimeoutController::parse(java_new<String>((char *)def))));
	return new SKSimpleTimeoutController(pSimpleTimeoutController);
}

SKSimpleTimeoutController::~SKSimpleTimeoutController()
{
	if(pImpl){
		delete pImpl;
		pImpl = NULL;
	}
}

SKSimpleTimeoutController::SKSimpleTimeoutController(int maxAttempts, int maxRelativeTimeoutMillis)
{
	pImpl = new SimpleTimeoutController(java_new<SimpleTimeoutController>(maxAttempts, maxRelativeTimeoutMillis )); 
}

SKSimpleTimeoutController::SKSimpleTimeoutController(SimpleTimeoutController * pSimpleTimeoutController)
{
	pImpl = pSimpleTimeoutController;
}

OpTimeoutController * SKSimpleTimeoutController::getPImpl()
{
	return static_cast< OpTimeoutController * >(pImpl);
}

int SKSimpleTimeoutController::getMaxAttempts(SKAsyncOperation * op)
{
	AsyncOperation* pAsyncOp = (AsyncOperation*) op->getPImpl();
	int maxAttempts = pImpl->getMaxAttempts(*pAsyncOp);
	return maxAttempts;
}

int SKSimpleTimeoutController::getRelativeTimeoutMillisForAttempt(SKAsyncOperation * op, int attemptIndex)
{
	AsyncOperation* pAsyncOp = (AsyncOperation*) op->getPImpl();
	int relativeTimeoutMillis = pImpl->getRelativeTimeoutMillisForAttempt(*pAsyncOp, attemptIndex);
	return relativeTimeoutMillis;
}

int SKSimpleTimeoutController::getMaxRelativeTimeoutMillis(SKAsyncOperation *op)
{
	AsyncOperation* pAsyncOp = (AsyncOperation*) op->getPImpl();
	int maxRelativeTimeoutMillis = pImpl->getMaxRelativeTimeoutMillis(*pAsyncOp);
	return maxRelativeTimeoutMillis;
}

SKSimpleTimeoutController * SKSimpleTimeoutController::maxAttempts(int maxAttempts)
{
	SimpleTimeoutController * pSimpleTimeoutController = 
		new SimpleTimeoutController(java_cast<SimpleTimeoutController>(
			pImpl->maxAttempts(maxAttempts)
		));
	delete pImpl;
    pImpl = pSimpleTimeoutController;
	return this;
}

SKSimpleTimeoutController * SKSimpleTimeoutController::maxRelativeTimeoutMillis(int maxRelativeTimeoutMillis)
{
	SimpleTimeoutController * pSimpleTimeoutController = 
		new SimpleTimeoutController(java_cast<SimpleTimeoutController>(
			pImpl->maxRelativeTimeoutMillis(maxRelativeTimeoutMillis)
		));
	delete pImpl;
    pImpl = pSimpleTimeoutController;
	return this;
}

string SKSimpleTimeoutController::toString()
{
	return (string) (pImpl->toString());
}

