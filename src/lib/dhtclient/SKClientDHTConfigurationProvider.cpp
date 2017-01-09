#include "SKClientDHTConfigurationProvider.h"
#include "SKClientDHTConfiguration.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;

#include "jace/proxy/com/ms/silverking/cloud/dht/client/ClientDHTConfigurationProvider.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::ClientDHTConfigurationProvider;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/ClientDHTConfiguration.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::ClientDHTConfiguration;


SKClientDHTConfigurationProvider::SKClientDHTConfigurationProvider(void * pClientDHTConfigurationProvider) { //FIXME: ?
	pImpl = pClientDHTConfigurationProvider;
}

SKClientDHTConfigurationProvider::~SKClientDHTConfigurationProvider() {
	if(pImpl) {
		ClientDHTConfigurationProvider* pClientDHTConfigurationProvider = (ClientDHTConfigurationProvider*) pImpl;
		delete pClientDHTConfigurationProvider;
		pImpl = NULL;
	}
};

void * SKClientDHTConfigurationProvider::getPImpl(){
	return pImpl;
}

SKClientDHTConfiguration * SKClientDHTConfigurationProvider::getClientDHTConfiguration(){
	ClientDHTConfigurationProvider* pClientDHTConfigurationProvider = (ClientDHTConfigurationProvider*) pImpl;
	ClientDHTConfiguration * pCdc = new ClientDHTConfiguration(
		java_cast<ClientDHTConfiguration>(pClientDHTConfigurationProvider->getClientDHTConfiguration()) );

	return new SKClientDHTConfiguration(pCdc);
}

