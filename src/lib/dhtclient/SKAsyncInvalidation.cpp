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
SKAsyncInvalidation::SKAsyncInvalidation() : pImpl(NULL) {};

/* public */
SKAsyncInvalidation::SKAsyncInvalidation(AsyncInvalidation * pAsyncInvalidation){
    pImpl = pAsyncInvalidation;
}
void *SKAsyncInvalidation::getPImpl() {
    return pImpl;
}

int64_t SKAsyncInvalidation::getStoredVersionI()
{
    return (int64_t)pImpl->getStoredVersion();
}

SKAsyncInvalidation::~SKAsyncInvalidation() { 
/*
    if (pImpl) {
        pImpl = NULL;
        delete pImpl;
    }
    */
};

