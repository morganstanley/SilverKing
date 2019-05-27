#include "SKAbsMillisVersionProvider.h"
#include "SKAbsMillisTimeSource.h"

#include "jace/Jace.h"
using jace::java_new;
using namespace jace;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AbsMillisVersionProvider.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AbsMillisVersionProvider;
#include "jace/proxy/com/ms/silverking/time/AbsMillisTimeSource.h"
using jace::proxy::com::ms::silverking::time::AbsMillisTimeSource;

SKAbsMillisVersionProvider::SKAbsMillisVersionProvider(AbsMillisVersionProvider * pAbsMillisVersionProvider) 
    : SKVersionProvider(pAbsMillisVersionProvider) {}; 
    
SKAbsMillisVersionProvider::~SKAbsMillisVersionProvider()
{
    if(pImpl!=NULL) {
        delete pImpl; 
        pImpl = NULL;
    }
}

SKAbsMillisVersionProvider::SKAbsMillisVersionProvider(SKAbsMillisTimeSource * absMillisTimeSource){
    AbsMillisTimeSource* pSource = (AbsMillisTimeSource*)absMillisTimeSource->getPImpl();
    pImpl = new AbsMillisVersionProvider(java_new<AbsMillisVersionProvider>(*pSource)); 
}
