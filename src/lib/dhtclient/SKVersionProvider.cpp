#include "SKVersionProvider.h"
#include "jace/proxy/com/ms/silverking/cloud/dht/client/VersionProvider.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::VersionProvider;

SKVersionProvider::SKVersionProvider(VersionProvider * pVPImpl) { //FIXME: ?
    if(pVPImpl)
        pImpl = pVPImpl;
}

SKVersionProvider::~SKVersionProvider() {
    if(pImpl) {
        delete pImpl;
        pImpl = NULL;
    }
};

int64_t SKVersionProvider::getVersion(){
    int64_t version = (int64_t)(this->getPImpl())->getVersion();
    return version;
}

VersionProvider * SKVersionProvider::getPImpl(){
    return pImpl;
}
