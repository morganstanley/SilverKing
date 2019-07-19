#include "SKSimpleSessionEstablishmentTimeoutController.h"
#include "SKSessionOptions.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;

#include "jace/proxy/com/ms/silverking/cloud/dht/client/SimpleSessionEstablishmentTimeoutController.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SimpleSessionEstablishmentTimeoutController;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/SessionEstablishmentTimeoutController.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SessionEstablishmentTimeoutController;
#include "jace/proxy/com/ms/silverking/cloud/dht/SessionOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::SessionOptions;


SKSimpleSessionEstablishmentTimeoutController * SKSimpleSessionEstablishmentTimeoutController::parse(const char * def)
{
    SimpleSessionEstablishmentTimeoutController * pSimpleTimeoutController = new SimpleSessionEstablishmentTimeoutController(java_cast<SimpleSessionEstablishmentTimeoutController>(
            SimpleSessionEstablishmentTimeoutController::parse(java_new<String>((char *)def))));
    return new SKSimpleSessionEstablishmentTimeoutController(pSimpleTimeoutController);
}

SKSimpleSessionEstablishmentTimeoutController::~SKSimpleSessionEstablishmentTimeoutController()
{
    if(pImpl){
        delete pImpl;
        pImpl = NULL;
    }
}

SKSimpleSessionEstablishmentTimeoutController::SKSimpleSessionEstablishmentTimeoutController(int maxAttempts, int attemptRelativeTimeoutMillis, int maxRelativeTimeoutMillis)
{
    pImpl = new SimpleSessionEstablishmentTimeoutController(java_new<SimpleSessionEstablishmentTimeoutController>(maxAttempts, attemptRelativeTimeoutMillis, maxRelativeTimeoutMillis )); 
}

SKSimpleSessionEstablishmentTimeoutController::SKSimpleSessionEstablishmentTimeoutController(SimpleSessionEstablishmentTimeoutController * pSimpleTimeoutController)
{
    pImpl = pSimpleTimeoutController;
}

SessionEstablishmentTimeoutController * SKSimpleSessionEstablishmentTimeoutController::getPImpl()
{
    //return static_cast< SessionEstablishmentTimeoutController * >(pImpl);
    return pImpl;
}

int SKSimpleSessionEstablishmentTimeoutController::getMaxAttempts(SKSessionOptions * pSessOpts)
{
    SessionOptions* pSOp = (SessionOptions*) pSessOpts->getPImpl();
    int maxAttempts = pImpl->getMaxAttempts(*pSOp);
    return maxAttempts;
}

int SKSimpleSessionEstablishmentTimeoutController::getRelativeTimeoutMillisForAttempt(SKSessionOptions * pSessOpts, int attemptIndex)
{
    SessionOptions* pSOp = (SessionOptions*) pSessOpts->getPImpl();
    int relativeTimeoutMillis = pImpl->getRelativeTimeoutMillisForAttempt(*pSOp, attemptIndex);
    return relativeTimeoutMillis;
}

int SKSimpleSessionEstablishmentTimeoutController::getMaxRelativeTimeoutMillis(SKSessionOptions * pSessOpts)
{
    SessionOptions* pSOp = (SessionOptions*) pSessOpts->getPImpl();
    int maxRelativeTimeoutMillis = pImpl->getMaxRelativeTimeoutMillis(*pSOp);
    return maxRelativeTimeoutMillis;
}

SKSimpleSessionEstablishmentTimeoutController * SKSimpleSessionEstablishmentTimeoutController::maxAttempts(int maxAttempts)
{
    SimpleSessionEstablishmentTimeoutController * pSimpleTimeoutController = 
        new SimpleSessionEstablishmentTimeoutController(java_cast<SimpleSessionEstablishmentTimeoutController>(
            pImpl->maxAttempts(maxAttempts)
        ));
    delete pImpl;
    pImpl = pSimpleTimeoutController;
    return this;
}

SKSimpleSessionEstablishmentTimeoutController * SKSimpleSessionEstablishmentTimeoutController::maxRelativeTimeoutMillis(int maxRelativeTimeoutMillis)
{
    SimpleSessionEstablishmentTimeoutController * pSimpleTimeoutController = 
        new SimpleSessionEstablishmentTimeoutController(java_cast<SimpleSessionEstablishmentTimeoutController>(
            pImpl->maxRelativeTimeoutMillis(maxRelativeTimeoutMillis)
        ));
    delete pImpl;
    pImpl = pSimpleTimeoutController;
    return this;
}

SKSimpleSessionEstablishmentTimeoutController *  SKSimpleSessionEstablishmentTimeoutController::attemptRelativeTimeoutMillis(int attemptRelativeTimeoutMillis)
{
    SimpleSessionEstablishmentTimeoutController * pSimpleTimeoutController = 
        new SimpleSessionEstablishmentTimeoutController(java_cast<SimpleSessionEstablishmentTimeoutController>(
            pImpl->attemptRelativeTimeoutMillis(attemptRelativeTimeoutMillis)
        ));
    delete pImpl;
    pImpl = pSimpleTimeoutController;
    return this;

}

string SKSimpleSessionEstablishmentTimeoutController::toString()
{
    return (string) (pImpl->toString());
}

