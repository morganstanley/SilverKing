/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#include "SKAsyncSnapshot.h"
#include "SKAsyncNSPerspective.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;

#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncSnapshot.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncSnapshot;

/* protected */
SKAsyncSnapshot::SKAsyncSnapshot(){};

/* public */
SKAsyncSnapshot::SKAsyncSnapshot(AsyncSnapshot * pAsyncSnapshot){
    pImpl = pAsyncSnapshot;
}
void * SKAsyncSnapshot::getPImpl() {
    return pImpl;
}

SKAsyncSnapshot::~SKAsyncSnapshot() { 
    if(pImpl ) {
        delete pImpl;
        pImpl = NULL;
    }
};


