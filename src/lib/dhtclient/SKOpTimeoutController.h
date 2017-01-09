#pragma once
#include "skconstants.h"
#include <limits.h>
#include <string>

class SKAsyncOperation;
namespace jace { namespace proxy { namespace com { namespace ms { 
	namespace silverking {namespace cloud { namespace dht { namespace client {
		class OpTimeoutController;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::OpTimeoutController OpTimeoutController;

class SKOpTimeoutController
{
public:
    SKAPI virtual int getMaxAttempts(SKAsyncOperation * op);
    SKAPI virtual int getRelativeTimeoutMillisForAttempt(SKAsyncOperation * op, int attemptIndex);
    SKAPI virtual int getMaxRelativeTimeoutMillis(SKAsyncOperation *op);
	SKAPI virtual std::string toString();
    
    static const int  INFINITE_REL_TIMEOUT = INT_MAX - 1;
    
	SKAPI ~SKOpTimeoutController();
	OpTimeoutController * getPImpl();
	SKOpTimeoutController(void *opTimeoutController);

protected:
	SKOpTimeoutController();
	void * pImpl;
};

