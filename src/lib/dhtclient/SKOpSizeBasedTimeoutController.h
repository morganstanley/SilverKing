#pragma once
#include "SKOpTimeoutController.h"

#include <string>
using std::string;

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class OpSizeBasedTimeoutController;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::OpSizeBasedTimeoutController OpSizeBasedTimeoutController;
namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class OpTimeoutController;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::OpTimeoutController OpTimeoutController;

class SKOpSizeBasedTimeoutController :     public SKOpTimeoutController
{
public:
    SKAPI static SKOpSizeBasedTimeoutController * parse(const char * def); 
    SKAPI virtual int getMaxAttempts(SKAsyncOperation * op);
    SKAPI virtual int getRelativeTimeoutMillisForAttempt(SKAsyncOperation * op, int attemptIndex);
    SKAPI virtual int getMaxRelativeTimeoutMillis(SKAsyncOperation *op);
    SKAPI SKOpSizeBasedTimeoutController * itemTimeMillis(int itemTimeMillis);
    SKAPI SKOpSizeBasedTimeoutController * constantTimeMillis(int constantTimeMillis);
    SKAPI SKOpSizeBasedTimeoutController * maxRelTimeoutMillis(int maxRelTimeoutMillis);
    SKAPI SKOpSizeBasedTimeoutController * maxAttempts(int maxAttempts);
    SKAPI string toString();

    SKAPI virtual ~SKOpSizeBasedTimeoutController();
    SKAPI SKOpSizeBasedTimeoutController();
    SKAPI SKOpSizeBasedTimeoutController(int maxAttempts, int constantTimeMillis, int itemTimeMillis, int maxRelTimeoutMillis);

    SKOpSizeBasedTimeoutController(OpSizeBasedTimeoutController * pOpSizeBasedTimeoutController);
    virtual OpTimeoutController * getPImpl();
private:
    OpSizeBasedTimeoutController * pImpl;
};

