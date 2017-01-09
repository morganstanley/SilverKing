/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#include "SKAsyncSyncRequest.h"
#include "SKAsyncNSPerspective.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;

#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncSyncRequest.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncSyncRequest;

/* protected */
SKAsyncSyncRequest::SKAsyncSyncRequest(){};

/* public */
SKAsyncSyncRequest::SKAsyncSyncRequest(AsyncSyncRequest * pAsyncSyncRequest){
    pImpl = pAsyncSyncRequest;
}
void * SKAsyncSyncRequest::getPImpl() {
    return pImpl;
}

SKAsyncSyncRequest::~SKAsyncSyncRequest() { 
    if(pImpl ) {
        delete pImpl;
        pImpl = NULL;
    }
};


