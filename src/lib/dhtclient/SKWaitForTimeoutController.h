#pragma once
#include "SKOpTimeoutController.h"

#include <string>
using std::string;

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class WaitForTimeoutController;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::WaitForTimeoutController WaitForTimeoutController;
namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class OpTimeoutController;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::OpTimeoutController OpTimeoutController;

class SKWaitForTimeoutController :     public SKOpTimeoutController
{
public:
    SKAPI virtual int getMaxAttempts(SKAsyncOperation * op);
    SKAPI virtual int getRelativeTimeoutMillisForAttempt(SKAsyncOperation * op, int attemptIndex);
    SKAPI virtual int getMaxRelativeTimeoutMillis(SKAsyncOperation *op);
    SKAPI string toString();

    SKAPI virtual ~SKWaitForTimeoutController();
    SKAPI SKWaitForTimeoutController();
    SKAPI SKWaitForTimeoutController(int internalRetryIntervalSeconds);

    SKWaitForTimeoutController(WaitForTimeoutController * pSimpleTimeoutController);
    virtual OpTimeoutController * getPImpl();
private:
    WaitForTimeoutController * pImpl;
};

