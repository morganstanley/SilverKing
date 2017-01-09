#include "SKOpSizeBasedTimeoutController.h"
#include "SKAsyncOperation.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;

#include "jace/proxy/com/ms/silverking/cloud/dht/client/OpSizeBasedTimeoutController.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::OpSizeBasedTimeoutController;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncOperation.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncOperation;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/OpTimeoutController.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::OpTimeoutController;

SKOpSizeBasedTimeoutController * SKOpSizeBasedTimeoutController::parse(const char * def)
{
	OpSizeBasedTimeoutController * pOpSizeBasedTimeoutController = new OpSizeBasedTimeoutController(java_cast<OpSizeBasedTimeoutController>(
			OpSizeBasedTimeoutController::parse(java_new<String>((char *)def))));
	return new SKOpSizeBasedTimeoutController(pOpSizeBasedTimeoutController);
}

SKOpSizeBasedTimeoutController::~SKOpSizeBasedTimeoutController()
{
	if(pImpl){
		delete pImpl;
		pImpl = NULL;
	}
}

SKOpSizeBasedTimeoutController::SKOpSizeBasedTimeoutController()
{
	pImpl = new OpSizeBasedTimeoutController(java_new<OpSizeBasedTimeoutController>()); 
}

SKOpSizeBasedTimeoutController::SKOpSizeBasedTimeoutController(int maxAttempts, int constantTimeMillis, int itemTimeMillis, int maxRelTimeoutMillis)
{
	pImpl = new OpSizeBasedTimeoutController(java_new<OpSizeBasedTimeoutController>(
					maxAttempts, constantTimeMillis, 
					itemTimeMillis, maxRelTimeoutMillis 
				)); 
}

SKOpSizeBasedTimeoutController::SKOpSizeBasedTimeoutController(OpSizeBasedTimeoutController * pOpSizeBasedTimeoutController)
{
	pImpl = pOpSizeBasedTimeoutController;
}

OpTimeoutController * SKOpSizeBasedTimeoutController::getPImpl()
{
	return static_cast<OpTimeoutController*>(pImpl);
}

int SKOpSizeBasedTimeoutController::getMaxAttempts(SKAsyncOperation * op)
{
	AsyncOperation* pAsyncOp = (AsyncOperation*) op->getPImpl();
	int maxAttempts = pImpl->getMaxAttempts(*pAsyncOp);
	return maxAttempts;
}

int SKOpSizeBasedTimeoutController::getRelativeTimeoutMillisForAttempt(SKAsyncOperation * op, int attemptIndex)
{
	AsyncOperation* pAsyncOp = (AsyncOperation*) op->getPImpl();
	int relativeTimeoutMillis = pImpl->getRelativeTimeoutMillisForAttempt(*pAsyncOp, attemptIndex);
	return relativeTimeoutMillis;
}

int SKOpSizeBasedTimeoutController::getMaxRelativeTimeoutMillis(SKAsyncOperation *op)
{
	AsyncOperation* pAsyncOp = (AsyncOperation*) op->getPImpl();
	int maxRelativeTimeoutMillis = pImpl->getMaxRelativeTimeoutMillis(*pAsyncOp);
	return maxRelativeTimeoutMillis;
}

SKOpSizeBasedTimeoutController * SKOpSizeBasedTimeoutController::constantTimeMillis(int constantTimeMillis)
{
	OpSizeBasedTimeoutController * pOpSizeBasedTimeoutController = 
		new OpSizeBasedTimeoutController(java_cast<OpSizeBasedTimeoutController>(
			pImpl->constantTimeMillis(constantTimeMillis)
		));
	delete pImpl;
    pImpl = pOpSizeBasedTimeoutController;
	return this;
}

SKOpSizeBasedTimeoutController * SKOpSizeBasedTimeoutController::itemTimeMillis(int itemTimeMillis)
{
	OpSizeBasedTimeoutController * pOpSizeBasedTimeoutController = 
		new OpSizeBasedTimeoutController(java_cast<OpSizeBasedTimeoutController>(
			pImpl->itemTimeMillis(itemTimeMillis)
		));
	delete pImpl;
    pImpl = pOpSizeBasedTimeoutController;
	return this;
}

SKOpSizeBasedTimeoutController * SKOpSizeBasedTimeoutController::maxRelTimeoutMillis(int maxRelTimeoutMillis)
{
	OpSizeBasedTimeoutController * pOpSizeBasedTimeoutController = 
		new OpSizeBasedTimeoutController(java_cast<OpSizeBasedTimeoutController>(
			pImpl->maxRelTimeoutMillis(maxRelTimeoutMillis)
		));
	delete pImpl;
    pImpl = pOpSizeBasedTimeoutController;
	return this;
}

SKOpSizeBasedTimeoutController * SKOpSizeBasedTimeoutController::maxAttempts(int maxAttempts)
{
	OpSizeBasedTimeoutController * pOpSizeBasedTimeoutController = 
		new OpSizeBasedTimeoutController(java_cast<OpSizeBasedTimeoutController>(
			pImpl->maxAttempts(maxAttempts)
		));
	delete pImpl;
    pImpl = pOpSizeBasedTimeoutController;
	return this;
}


string SKOpSizeBasedTimeoutController::toString()
{
	return (string) (pImpl->toString());
}

