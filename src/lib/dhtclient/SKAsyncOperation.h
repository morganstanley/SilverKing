/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKASYNCOPERATION_H
#define SKASYNCOPERATION_H

#include "skconstants.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
	namespace silverking {namespace cloud { namespace dht { namespace client {
		class AsyncOperation;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::AsyncOperation AsyncOperation;


class SKAsyncOperation 
{
public:
	SKAPI SKOperationState::SKOperationState getState();
	SKAPI SKFailureCause::SKFailureCause getFailureCause();
	SKAPI virtual void waitForCompletion();
	SKAPI virtual bool waitForCompletion(long timeout, SKTimeUnit unit);
	SKAPI void close();
	SKAPI virtual ~SKAsyncOperation();
	
    SKAsyncOperation(AsyncOperation * pAsyncOperation);
	virtual void * getPImpl();
protected:
    AsyncOperation * pImpl;

    SKAsyncOperation();
    void repackException(const char * fileName , int lineNum );
};


#endif //SKASYNCOPERATION_H
