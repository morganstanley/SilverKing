#ifndef SKASYNCNSPERSPECTIVE_H
#define SKASYNCNSPERSPECTIVE_H

#include <map>
using std::map;
#include "skconstants.h"
#include "skbasictypes.h"
#include "SKAsyncReadableNSPerspective.h"
#include "SKAsyncWritableNSPerspective.h"
#include "SKInvalidationOptions.h"
#include "SKRetrievalOptions.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
	namespace silverking {namespace cloud { namespace dht { namespace client {
		class AsynchronousNamespacePerspective;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::AsynchronousNamespacePerspective AsynchronousNamespacePerspective;

class SKAsyncNSPerspective : public SKAsyncReadableNSPerspective, public SKAsyncWritableNSPerspective
{
public:
	SKAPI ~SKAsyncNSPerspective();
	SKAPI void waitForActiveOps();

	SKAPI virtual SKAsyncPut * put(SKMap<string, SKVal*> const * dhtValues);
	SKAPI virtual SKAsyncPut * put(SKMap<string, SKVal*> const * dhtValues, SKPutOptions * putOptions);
	SKAPI virtual SKAsyncPut * put(const char * key, const SKVal * value, SKPutOptions * putOptions);
	SKAPI virtual SKAsyncPut * put(const char * key, const SKVal * value);
    SKAPI virtual SKAsyncPut * put(string * key, const SKVal * value, SKPutOptions * putOptions);
    SKAPI virtual SKAsyncPut * put(string * key, const SKVal * value);

	SKAPI virtual SKAsyncInvalidation *invalidate(SKVector<std::string> const * dhtKeys);
	SKAPI virtual SKAsyncInvalidation *invalidate(SKVector<std::string> const * dhtKeys, SKInvalidationOptions * invalidationOptions);
	SKAPI virtual SKAsyncInvalidation *invalidate(const char * key, SKInvalidationOptions * invalidationOptions);
	SKAPI virtual SKAsyncInvalidation *invalidate(const char * key);
    SKAPI virtual SKAsyncInvalidation *invalidate(string * key, SKInvalidationOptions * invalidationOptions);
    SKAPI virtual SKAsyncInvalidation *invalidate(string * key);
	
	SKAPI virtual SKAsyncValueRetrieval * get(SKVector<std::string> const * dhtKeys);
	SKAPI virtual SKAsyncRetrieval * get(SKVector<std::string> const * dhtKeys, SKGetOptions * getOptions);
	SKAPI virtual SKAsyncRetrieval * get(const char * key, SKGetOptions * getOptions);
	SKAPI virtual SKAsyncSingleValueRetrieval * get(const char * key);
    SKAPI virtual SKAsyncRetrieval * get(string * key, SKGetOptions * getOptions);
    SKAPI virtual SKAsyncSingleValueRetrieval * get(string * key);

	SKAPI virtual SKAsyncValueRetrieval * waitFor(SKVector<std::string> const * dhtKeys);
	SKAPI virtual SKAsyncRetrieval * waitFor(SKVector<std::string> const * dhtKeys, SKWaitOptions * waitOptions);
	SKAPI virtual SKAsyncRetrieval * waitFor(const char * key, SKWaitOptions * waitOptions);
	SKAPI virtual SKAsyncSingleValueRetrieval * waitFor(const char * key);
    SKAPI virtual SKAsyncRetrieval * waitFor(string * key, SKWaitOptions * waitOptions);
    SKAPI virtual SKAsyncSingleValueRetrieval * waitFor(string * key);
	
	SKAsyncNSPerspective(AsynchronousNamespacePerspective * pAsyncNSPerspective);
	void * getPImpl();
private:
	AsynchronousNamespacePerspective * pImpl;
    SKAsyncRetrieval * _retrieve(SKVector<string> const * dhtKeys, SKRetrievalOptions * retrOptions, bool isWaitFor);
    SKAsyncValueRetrieval * _retrieve(SKVector<string> const * dhtKeys, bool isWaitFor);
};


#endif  //SKASYNCNSPERSPECTIVE_H
