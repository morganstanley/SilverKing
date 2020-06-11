/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#include "SKAsyncPut.h"
#include "SKAsyncNSPerspective.h"
#include "SKStoredValue.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncPut.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncPut;


/* protected */
SKAsyncPut::SKAsyncPut() : pImpl(NULL) {};

/* public */
SKAsyncPut::SKAsyncPut(AsyncPut * pAsyncPut){
    pImpl = pAsyncPut;
}

void * SKAsyncPut::getPImpl() {
    return pImpl;
}

int64_t SKAsyncPut::getStoredVersion()
{
    return (int64_t)pImpl->getStoredVersion();
}

SKAsyncPut::~SKAsyncPut() { 
    if (pImpl) {
        delete pImpl;
        pImpl = NULL;
    }
};
