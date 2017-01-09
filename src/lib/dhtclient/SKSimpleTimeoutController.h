#pragma once
#include "SKOpTimeoutController.h"

#include <string>
using std::string;

namespace jace { namespace proxy { namespace com { namespace ms { 
	namespace silverking {namespace cloud { namespace dht { namespace client {
		class SimpleTimeoutController;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::SimpleTimeoutController SimpleTimeoutController;
namespace jace { namespace proxy { namespace com { namespace ms { 
	namespace silverking {namespace cloud { namespace dht { namespace client {
		class OpTimeoutController;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::OpTimeoutController OpTimeoutController;

class SKSimpleTimeoutController : public SKOpTimeoutController
{
public:
	SKAPI static SKSimpleTimeoutController * parse(const char * def); 
    SKAPI virtual int getMaxAttempts(SKAsyncOperation * op);
    SKAPI virtual int getRelativeTimeoutMillisForAttempt(SKAsyncOperation * op, int attemptIndex);
    SKAPI virtual int getMaxRelativeTimeoutMillis(SKAsyncOperation *op);
	SKAPI SKSimpleTimeoutController * maxAttempts(int maxAttempts);
	SKAPI SKSimpleTimeoutController * maxRelativeTimeoutMillis(int maxRelativeTimeoutMillis);
	SKAPI string toString();

	SKAPI virtual ~SKSimpleTimeoutController();
	SKAPI SKSimpleTimeoutController(int maxAttempts, int maxRelativeTimeoutMillis);
	SKSimpleTimeoutController(SimpleTimeoutController * pSimpleTimeoutController);
	virtual OpTimeoutController * getPImpl();
private:
	SimpleTimeoutController * pImpl;
};

