/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKASYNCSNSYNCREQUEST_H
#define SKASYNCSNSYNCREQUEST_H

#include "skconstants.h"
#include "skbasictypes.h"
#include "SKAsyncOperation.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class AsyncSyncRequest;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::AsyncSyncRequest AsyncSyncRequest;


class SKAsyncSyncRequest : public SKAsyncOperation
{
public:
    SKAPI virtual ~SKAsyncSyncRequest();

       SKAsyncSyncRequest(AsyncSyncRequest * pAsyncSyncRequest);
    void * getPImpl();
protected:
    SKAsyncSyncRequest();
    SKAsyncSyncRequest(const SKAsyncSyncRequest & );
    const SKAsyncSyncRequest& operator= (const SKAsyncSyncRequest & );  
};

#endif  //SKASYNCSNSYNCREQUEST_H

