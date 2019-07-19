#pragma once
#include "SKSessionEstablishmentTimeoutController.h"

#include <string>
using std::string;

class SKSessionOptions;
namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class SimpleSessionEstablishmentTimeoutController;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::SimpleSessionEstablishmentTimeoutController SimpleSessionEstablishmentTimeoutController;
namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class SessionEstablishmentTimeoutController;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::SessionEstablishmentTimeoutController SessionEstablishmentTimeoutController;

class SKSimpleSessionEstablishmentTimeoutController : public SKSessionEstablishmentTimeoutController
{
public:
    SKAPI static SKSimpleSessionEstablishmentTimeoutController * parse(const char * def); 
    SKAPI virtual ~SKSimpleSessionEstablishmentTimeoutController();
    SKAPI SKSimpleSessionEstablishmentTimeoutController(int maxAttempts, int attemptRelativeTimeoutMillis, int maxRelativeTimeoutMillis);
    SKAPI virtual int getMaxAttempts(SKSessionOptions * pSessOpts);
    SKAPI virtual int getRelativeTimeoutMillisForAttempt(SKSessionOptions * pSessOpts, int attemptIndex);
    SKAPI virtual int getMaxRelativeTimeoutMillis(SKSessionOptions * pSessOpts);
    SKAPI SKSimpleSessionEstablishmentTimeoutController * maxAttempts(int maxAttempts);
    SKAPI SKSimpleSessionEstablishmentTimeoutController * maxRelativeTimeoutMillis(int maxRelativeTimeoutMillis);
    SKAPI SKSimpleSessionEstablishmentTimeoutController * attemptRelativeTimeoutMillis(int attemptRelativeTimeoutMillis);
    SKAPI string toString();


    SKSimpleSessionEstablishmentTimeoutController(SimpleSessionEstablishmentTimeoutController * pSimpleSessionEstablishmentTimeoutController);
    virtual SessionEstablishmentTimeoutController * getPImpl();
private:
    SimpleSessionEstablishmentTimeoutController * pImpl;
};

