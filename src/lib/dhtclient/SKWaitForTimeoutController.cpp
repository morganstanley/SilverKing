#include "SKWaitForTimeoutController.h"
#include "SKAsyncOperation.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;

#include "jace/proxy/com/ms/silverking/cloud/dht/client/WaitForTimeoutController.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::WaitForTimeoutController;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncOperation.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncOperation;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/OpTimeoutController.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::OpTimeoutController;

SKWaitForTimeoutController::~SKWaitForTimeoutController()
{
    if(pImpl){
        delete pImpl;
        pImpl = NULL;
    }
}

SKWaitForTimeoutController::SKWaitForTimeoutController()
{
    pImpl = new WaitForTimeoutController(java_new<WaitForTimeoutController>()); 
}

SKWaitForTimeoutController::SKWaitForTimeoutController(int internalRetryIntervalSeconds)
{
    pImpl = new WaitForTimeoutController(java_new<WaitForTimeoutController>(internalRetryIntervalSeconds)); 
}

SKWaitForTimeoutController::SKWaitForTimeoutController(WaitForTimeoutController * pWaitForTimeoutController)
{
    pImpl = pWaitForTimeoutController;
}

OpTimeoutController * SKWaitForTimeoutController::getPImpl()
{
    return static_cast<OpTimeoutController*>(pImpl);
}

int SKWaitForTimeoutController::getMaxAttempts(SKAsyncOperation * op)
{
    AsyncOperation* pAsyncOp = (AsyncOperation*) op->getPImpl();
    int maxAttempts = pImpl->getMaxAttempts(*pAsyncOp);
    return maxAttempts;
}

int SKWaitForTimeoutController::getRelativeTimeoutMillisForAttempt(SKAsyncOperation * op, int attemptIndex)
{
    AsyncOperation* pAsyncOp = (AsyncOperation*) op->getPImpl();
    int relativeTimeoutMillis = pImpl->getRelativeTimeoutMillisForAttempt(*pAsyncOp, attemptIndex);
    return relativeTimeoutMillis;
}

int SKWaitForTimeoutController::getMaxRelativeTimeoutMillis(SKAsyncOperation *op)
{
    AsyncOperation* pAsyncOp = (AsyncOperation*) op->getPImpl();
    int maxRelativeTimeoutMillis = pImpl->getMaxRelativeTimeoutMillis(*pAsyncOp);
    return maxRelativeTimeoutMillis;
}

string SKWaitForTimeoutController::toString()
{
    return (string) (pImpl->toString());
}

