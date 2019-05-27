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
SKAsyncPut::SKAsyncPut(){};

/* public */
SKAsyncPut::SKAsyncPut(AsyncPut * pAsyncPut){
    pImpl = pAsyncPut;
}
void * SKAsyncPut::getPImpl() {
    return pImpl;
}

SKAsyncPut::~SKAsyncPut() { 
    if(pImpl ) {
        AsyncPut * pPut = dynamic_cast<AsyncPut*>(pImpl);
        pImpl = NULL;
        delete pPut;
    }
};

