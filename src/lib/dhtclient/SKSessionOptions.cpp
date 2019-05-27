#include "skbasictypes.h"
#include "SKSessionOptions.h"
#include "SKClientDHTConfiguration.h"
#include "SKSessionEstablishmentTimeoutController.h"
#include "SKSimpleSessionEstablishmentTimeoutController.h"
#include <string>
using std::string;

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/com/ms/silverking/cloud/dht/SessionOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::SessionOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/ClientDHTConfiguration.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::ClientDHTConfiguration;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/SessionEstablishmentTimeoutController.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SessionEstablishmentTimeoutController;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/SimpleSessionEstablishmentTimeoutController.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SimpleSessionEstablishmentTimeoutController;

SKSessionEstablishmentTimeoutController * SKSessionOptions::getDefaultTimeoutController()
{
    SimpleSessionEstablishmentTimeoutController * pSessionEstablishmentTimeoutController = 
        new SimpleSessionEstablishmentTimeoutController(java_cast<SimpleSessionEstablishmentTimeoutController>(
            SessionOptions::getDefaultTimeoutController()));
    return new SKSimpleSessionEstablishmentTimeoutController(pSessionEstablishmentTimeoutController);
}

SKSessionOptions::SKSessionOptions(SKClientDHTConfiguration * dhtConfig, const char * preferredServer,
            SKSessionEstablishmentTimeoutController * pTimeoutController)
{
    ClientDHTConfiguration * pClientDhtConf = ((ClientDHTConfiguration *) dhtConfig->getPImpl());
    SessionEstablishmentTimeoutController * pController = ((SessionEstablishmentTimeoutController *) pTimeoutController->getPImpl());
    pImpl = new SessionOptions(java_new<SessionOptions>(*pClientDhtConf, 
                            java_new<String>((char*)preferredServer), *pController )); 
}

SKSessionOptions::SKSessionOptions(SKClientDHTConfiguration * dhtConfig, const char * preferredServer) 
{
    ClientDHTConfiguration * pClientDhtConf = ((ClientDHTConfiguration *) dhtConfig->getPImpl());
    pImpl = new SessionOptions(java_new<SessionOptions>(*pClientDhtConf, 
                            java_new<String>((char*)preferredServer))); 
}

SKSessionOptions::SKSessionOptions(SKClientDHTConfiguration * dhtConfig){
    ClientDHTConfiguration * pClientDhtConf = ((ClientDHTConfiguration *) dhtConfig->getPImpl());
    pImpl = new SessionOptions(java_new<SessionOptions>(*pClientDhtConf) ); 
}


SKSessionOptions::SKSessionOptions(void * pOpt) : pImpl(pOpt) {};  //FIXME: make protected ?

SKSessionOptions::~SKSessionOptions()
{
    if(pImpl!=NULL) {
        SessionOptions * po = (SessionOptions*)pImpl;
        delete po; 
        pImpl = NULL;
    }
}

void * SKSessionOptions::getPImpl(){
    return pImpl;
}

char * SKSessionOptions::toString(){
    string representation = (string)(((SessionOptions*)pImpl)->toString());
    return skStrDup(representation.c_str(),__FILE__, __LINE__);
}

SKClientDHTConfiguration * SKSessionOptions::getDHTConfig(){
    ClientDHTConfiguration * pClientDHTConf = new ClientDHTConfiguration(java_cast<ClientDHTConfiguration>(
        ((SessionOptions*)pImpl)->getDHTConfig()
    )); 
    return new SKClientDHTConfiguration(pClientDHTConf); 
}

char * SKSessionOptions::getPreferredServer(){
    string server = (string)((SessionOptions*)pImpl)->getPreferredServer();
    return skStrDup(server.c_str(),__FILE__, __LINE__);
}

SKSessionOptions * SKSessionOptions::preferredServer(const char * preferredServer){
    SessionOptions * pSessOptImp = new SessionOptions(java_cast<SessionOptions>(
        ((SessionOptions*)pImpl)->preferredServer(java_new<String>((char*)preferredServer))
    )); 
    delete ((SessionOptions*)pImpl);
    pImpl = pSessOptImp;
    return this;
} 


void SKSessionOptions::setDefaultTimeoutController(SKSessionEstablishmentTimeoutController * pDefaultTimeoutController)
{
    SessionEstablishmentTimeoutController * pController = pDefaultTimeoutController->getPImpl();
    ((SessionOptions*)pImpl)->setDefaultTimeoutController(*pController);
    
}

SKSessionEstablishmentTimeoutController * SKSessionOptions::getTimeoutController()
{
    SimpleSessionEstablishmentTimeoutController * pSessionEstablishmentTimeoutController = 
        new SimpleSessionEstablishmentTimeoutController(java_cast<SimpleSessionEstablishmentTimeoutController>(
            ((SessionOptions*)pImpl)->getTimeoutController()));
    return new SKSimpleSessionEstablishmentTimeoutController(pSessionEstablishmentTimeoutController);
}

