#include "SKClientDHTConfiguration.h"
#include "skbasictypes.h"
#include "SKAddrAndPort.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/proxy/java/lang/String.h"
using ::jace::proxy::java::lang::String;
#include "jace/proxy/java/lang/Object.h"
using ::jace::proxy::java::lang::Object;
#include "jace/proxy/java/io/File.h"
using ::jace::proxy::java::io::File;
#include "jace/proxy/java/util/Set.h"
using jace::proxy::java::util::Set;
#include "jace/proxy/java/util/List.h"
using jace::proxy::java::util::List;
#include "jace/proxy/java/util/Map.h"
using jace::proxy::java::util::Map;
#include "jace/proxy/java/util/HashMap.h"
using jace::proxy::java::util::HashMap;
#include "jace/proxy/java/util/Map_Entry.h"
using jace::proxy::java::util::Map_Entry;
#include "jace/proxy/java/util/Iterator.h"
using jace::proxy::java::util::Iterator;

#include "jace/proxy/com/ms/silverking/cloud/dht/client/ClientDHTConfigurationProvider.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::ClientDHTConfigurationProvider;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/ClientDHTConfiguration.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::ClientDHTConfiguration;
#include "jace/proxy/com/ms/silverking/net/AddrAndPort.h"
using jace::proxy::com::ms::silverking::net::AddrAndPort;
#include "jace/proxy/com/ms/silverking/net/HostAndPort.h"
using jace::proxy::com::ms::silverking::net::HostAndPort;

typedef JArray< jace::proxy::com::ms::silverking::net::AddrAndPort > AddrAndPortArray;


SKClientDHTConfiguration * SKClientDHTConfiguration::create(map<string,string> * envMap){
	Map values = java_new<HashMap>();
	map<string,string>::iterator it;
    for(it = envMap->begin() ; it != envMap->end(); it++ ){
		values.put(String(it->first), String(it->second));
    }

	ClientDHTConfiguration * pCdc = new ClientDHTConfiguration( java_cast<ClientDHTConfiguration>(
		ClientDHTConfiguration::create(values)
	));
	
	return new SKClientDHTConfiguration(pCdc);
}

#if 0
SKClientDHTConfiguration::SKClientDHTConfiguration(const char * dhtName, int dhtPort, SKAddrAndPort zkLocs[])
	: SKClientDHTConfigurationProvider((void*)NULL)
{
	String str = java_new<String>((char *)dhtName);
	int zkLocsLength = sizeof(zkLocs)/sizeof(SKAddrAndPort);
	AddrAndPortArray addrAndPortArray(zkLocsLength);
	for(int i=0; i<zkLocsLength; ++i) {
		AddrAndPort* pAddrPort = (AddrAndPort*)((zkLocs[i]).getPImpl());
		addrAndPortArray[i] = *pAddrPort;
	}
	pImpl = new ClientDHTConfiguration( java_new<ClientDHTConfiguration> ( str, dhtPort, addrAndPortArray ));
}

SKClientDHTConfiguration::SKClientDHTConfiguration(const char * dhtName, SKAddrAndPort zkLocs[])
	: SKClientDHTConfigurationProvider((void*)NULL)
{
	String str = java_new<String>((char *)dhtName);
	int zkLocsLength = sizeof(zkLocs)/sizeof(SKAddrAndPort);
	AddrAndPortArray addrAndPortArray(zkLocsLength);
	for(int i=0; i<zkLocsLength; ++i) {
		AddrAndPort* pAddrPort = (AddrAndPort*)(zkLocs[i].getPImpl());
		addrAndPortArray[i] = *pAddrPort;
	}
	pImpl = new ClientDHTConfiguration( java_new<ClientDHTConfiguration> ( str, addrAndPortArray ));
}
#endif

SKClientDHTConfiguration::SKClientDHTConfiguration(const char * dhtName, const char * zkLocs)
	: SKClientDHTConfigurationProvider((void*)NULL)
{
	String name = java_new<String>((char *)dhtName);
	String locs = java_new<String>((char *)zkLocs);
	pImpl = new ClientDHTConfiguration( java_new<ClientDHTConfiguration> ( name, locs ));

}

SKClientDHTConfiguration::SKClientDHTConfiguration(const char * dhtName, int dhtPort, const char * zkLocs)
	: SKClientDHTConfigurationProvider((void*)NULL)
{
	String name = java_new<String>((char *)dhtName);
	String locs = java_new<String>((char *)zkLocs);
	pImpl = new ClientDHTConfiguration( java_new<ClientDHTConfiguration> ( name, dhtPort, locs ));
}

SKClientDHTConfiguration::SKClientDHTConfiguration(void * pClientDHTConfiguration) //FIXME: ?
	: SKClientDHTConfigurationProvider((void*)NULL)
{
	if(pClientDHTConfiguration)
		pImpl = pClientDHTConfiguration;
}

SKClientDHTConfiguration::~SKClientDHTConfiguration() {
	if(pImpl) {
		ClientDHTConfiguration* pClientDHTConfiguration = (ClientDHTConfiguration*) pImpl;
		delete pClientDHTConfiguration;
		pImpl = NULL;
	}
};

/*
void * SKClientDHTConfiguration::getPImpl(){
	return pImpl;
}
*/


char * SKClientDHTConfiguration::getName(){
	ClientDHTConfiguration* pCdc = (ClientDHTConfiguration*) pImpl;
	string str = (string) java_cast<String>(pCdc->getName());
	return skStrDup(str.c_str(),__FILE__, __LINE__);
}

char * SKClientDHTConfiguration::toString(){
	ClientDHTConfiguration* pCdc = (ClientDHTConfiguration*) pImpl;
	string str = (string) java_cast<String>(pCdc->toString());
	return skStrDup(str.c_str(),__FILE__, __LINE__);
}

int SKClientDHTConfiguration::getPort(){
	ClientDHTConfiguration* pCdc = (ClientDHTConfiguration*) pImpl;
	int port = (int) pCdc->getPort();
	return port;
}

bool SKClientDHTConfiguration::hasPort(){
	ClientDHTConfiguration* pCdc = (ClientDHTConfiguration*) pImpl;
	bool hasPort = (bool) pCdc->hasPort();
	return hasPort;
}

#if 0
SKAddrAndPort* SKClientDHTConfiguration::getZkLocs(){
	ClientDHTConfiguration* pCdc = (ClientDHTConfiguration*) pImpl;
	AddrAndPortArray jlocs = java_cast<AddrAndPortArray>(pCdc->getZkLocs());
	//if ( jlocs.isNull() ) { /* ... */ }
	
	const size_t locsLength = jlocs.length();
	SKAddrAndPort * locs = new SKAddrAndPort[locsLength];
	
	for ( size_t i = 0; i < locsLength; ++i ){
		AddrAndPort * pAddrAndPort = new AddrAndPort( jlocs[i] );
		new(&locs[i]) SKAddrAndPort( pAddrAndPort ); //obj allocated using placement new(where)
	}
	return locs;
}
#endif

SKClientDHTConfiguration * SKClientDHTConfiguration::getClientDHTConfiguration(){
	ClientDHTConfiguration* pClientConf = (ClientDHTConfiguration*) pImpl;
	ClientDHTConfiguration * pCdc = new ClientDHTConfiguration(
		java_cast<ClientDHTConfiguration>(pClientConf->getClientDHTConfiguration()) );

	return new SKClientDHTConfiguration(pCdc);
}
