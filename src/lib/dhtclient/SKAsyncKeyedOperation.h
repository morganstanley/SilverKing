/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKASYNCKEYEDOPERATION_H
#define SKASYNCKEYEDOPERATION_H

#include "skconstants.h"
#include "skcontainers.h"
#include "skbasictypes.h"
#include "SKAsyncOperation.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class AsyncKeyedOperation;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::AsyncKeyedOperation AsyncKeyedOperation;


class SKAsyncKeyedOperation : public SKAsyncOperation
{
public:
    SKAPI virtual ~SKAsyncKeyedOperation();
    SKAPI SKVector<string> * getKeys(void);
    SKAPI SKOperationState::SKOperationState getOperationState(const string& key);
    SKAPI SKMap<std::string, SKOperationState::SKOperationState> * getOperationStateMap();
    SKAPI virtual SKVector<string> * getIncompleteKeys();
    SKAPI virtual int getNumKeys();

    SKAsyncKeyedOperation(AsyncKeyedOperation * pAsyncOperation);
    void * getPImpl();
protected:
    SKAsyncKeyedOperation();

};

#endif  //SKASYNCKEYEDOPERATION_H

