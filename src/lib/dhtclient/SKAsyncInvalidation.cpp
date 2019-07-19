#include "SKAsyncInvalidation.h"
#include "SKAsyncNSPerspective.h"
#include "SKStoredValue.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncInvalidation.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncInvalidation;

/* protected */
SKAsyncInvalidation::SKAsyncInvalidation(){};

/* public */
SKAsyncInvalidation::SKAsyncInvalidation(AsyncInvalidation * pAsyncInvalidation){
    pImpl = pAsyncInvalidation;
}
void *SKAsyncInvalidation::getPImpl() {
    return pImpl;
}

SKAsyncInvalidation::~SKAsyncInvalidation() { 
    if (pImpl) {
        AsyncInvalidation * pInvalidation = dynamic_cast<AsyncInvalidation*>(pImpl);
        pImpl = NULL;
        delete pInvalidation;
    }
};

