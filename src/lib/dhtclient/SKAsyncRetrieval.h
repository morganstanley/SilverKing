/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKASYNCRETRIEVAL_H
#define SKASYNCRETRIEVAL_H

#include "skconstants.h"
#include "SKAsyncKeyedOperation.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
	namespace silverking {namespace cloud { namespace dht { namespace client {
		class AsyncRetrieval;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::AsyncRetrieval AsyncRetrieval;

class SKStoredValue;

class SKAsyncRetrieval : public SKAsyncKeyedOperation
{
public:
    SKAPI SKMap<string,SKStoredValue * > *  getLatestStoredValues();
    SKAPI SKMap<string,SKStoredValue * > *  getStoredValues();
    SKAPI SKStoredValue *  getStoredValue(string& key);
	SKAPI virtual ~SKAsyncRetrieval();

	SKAsyncRetrieval(AsyncRetrieval * pAsyncRetrieval);
    void * getPImpl();
protected:
    SKMap<string,SKStoredValue * > * _getStoredValues(bool latest);
    SKAsyncRetrieval();
    SKAsyncRetrieval(const SKAsyncRetrieval & );
    const SKAsyncRetrieval& operator= (const SKAsyncRetrieval & );
};

#endif  //SKASYNCRETRIEVAL_H

