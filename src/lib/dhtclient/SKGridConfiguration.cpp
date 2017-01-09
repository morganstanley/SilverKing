#include "SKGridConfiguration.h"
#include "SKClientDHTConfiguration.h"
#include "skbasictypes.h"

#include <string.h>
#include <string>
using std::string;
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
#include "jace/proxy/com/ms/silverking/cloud/dht/gridconfig/SKGridConfiguration.h"
//using jace::proxy::com::ms::silverking::cloud::dht::gridconfig::SKGridConfiguration;
namespace jgc = jace::proxy::com::ms::silverking::cloud::dht::gridconfig;  //namespace aliasing


SKGridConfiguration * SKGridConfiguration::parseFile(const char * gcBaseFile, const char * gcName){
	File gcBase = java_new<File>(java_new<String>((char *)gcBaseFile));
	
	jgc::SKGridConfiguration * pGc = new jgc::SKGridConfiguration( java_cast<jgc::SKGridConfiguration>(
		jgc::SKGridConfiguration::parseFile(gcBase, java_new<String>((char *)gcName))
	));
	return new SKGridConfiguration(pGc);
}

SKGridConfiguration * SKGridConfiguration::parseFile(const char * gcName){
	jgc::SKGridConfiguration * pGc = new jgc::SKGridConfiguration( java_cast<jgc::SKGridConfiguration>(
		jgc::SKGridConfiguration::parseFile(java_new<String>((char *)gcName))
	));
	return new SKGridConfiguration(pGc);
}

map<string, string> * SKGridConfiguration::readEnvFile(const char * envFileName){
	File envFile = java_new<File>(java_new<String>((char *)envFileName));
	Map envVars =  java_cast<Map>( jgc::SKGridConfiguration::readEnvFile(envFile) );
	map<string, string> * pResult = new map<string, string>();
	
	Set entrySet(envVars.entrySet());
	for (Iterator it(entrySet.iterator()); it.hasNext(); )
	{
		Map_Entry entry = java_cast<Map_Entry>(it.next());
		String key = java_cast<String>(entry.getKey());
		String value = java_cast<String>(entry.getValue());
		pResult->insert(map<string, string>::value_type((string)key, (string)value));
	}
	
	return pResult;
}

SKGridConfiguration::SKGridConfiguration(const char * name, map<string,string> * envMap)
	: SKClientDHTConfigurationProvider((void*)NULL)
{
	Map values = java_new<HashMap>();
	map<string,string>::iterator it;
    for(it = envMap->begin() ; it != envMap->end(); it++ ){
		values.put(String(it->first), String(it->second));
    }

	jgc::SKGridConfiguration * pGC = new jgc::SKGridConfiguration( java_new<jgc::SKGridConfiguration>(java_new<String>((char *)name), values	));
	pImpl = pGC;
} 


SKGridConfiguration::SKGridConfiguration(void * pGridConfiguration)  //FIXME: ?
	: SKClientDHTConfigurationProvider((void*)NULL)
{
	if(pGridConfiguration)
		pImpl = pGridConfiguration;
}

SKGridConfiguration::~SKGridConfiguration() {
	if(pImpl) {
		jgc::SKGridConfiguration * pGridConfiguration = (jgc::SKGridConfiguration*) pImpl;
		delete pGridConfiguration;
		pImpl = NULL;
	}
};

/*
void * SKGridConfiguration::getPImpl(){
	return pImpl;
}
*/

char * SKGridConfiguration::getName(){
	jgc::SKGridConfiguration * pGc = (jgc::SKGridConfiguration*) pImpl;
	string str = (string) java_cast<String>(pGc->getName());
	return skStrDup(str.c_str(),__FILE__, __LINE__);
}

char * SKGridConfiguration::get(const char * envKey){
	jgc::SKGridConfiguration* pGc = (jgc::SKGridConfiguration*) pImpl;
	string str = (string) java_cast<String>(pGc->get(java_new<String>((char *)envKey)));
	return skStrDup(str.c_str(),__FILE__, __LINE__);
}

char * SKGridConfiguration::toString(){
	jgc::SKGridConfiguration* pGc = (jgc::SKGridConfiguration*) pImpl;
	string str = (string) java_cast<String>(pGc->toString());
	return skStrDup(str.c_str(),__FILE__, __LINE__);
}

map<string,string> * SKGridConfiguration::getEnvMap(){
	jgc::SKGridConfiguration* pGc = (jgc::SKGridConfiguration*) pImpl;
	Map envVars =  java_cast<Map>( pGc->getEnvMap() );
	map<string, string> * pResult = new map<string, string>();
	
	Set entrySet(envVars.entrySet());
	for (Iterator it(entrySet.iterator()); it.hasNext(); )
	{
		Map_Entry entry = java_cast<Map_Entry>(it.next());
		String key = java_cast<String>(entry.getKey());
		String value = java_cast<String>(entry.getValue());
		pResult->insert(map<string, string>::value_type((string)key, (string)value));
	}
	
	return pResult;
}

/*
SKClientDHTConfiguration * SKGridConfiguration::getClientDHTConfiguration(){
	GridConfiguration* pGridConfiguration = (GridConfiguration*) pImpl;
	ClientDHTConfiguration * pCdc = new ClientDHTConfiguration(
		java_cast<ClientDHTConfiguration>(pGridConfiguration->getClientDHTConfiguration()) );

	return new SKClientDHTConfiguration(pCdc);
}
*/

/* Not supported */
/* SKG2Configuration * SKGridConfiguration::getG2Configuration() {} */

	
SKClientDHTConfiguration * SKGridConfiguration::getClientDHTConfiguration(){
	jgc::SKGridConfiguration* pGridConf = (jgc::SKGridConfiguration*) pImpl;
	ClientDHTConfiguration * pCdc = new ClientDHTConfiguration(
		java_cast<ClientDHTConfiguration>(pGridConf->getClientDHTConfiguration()) );

	return new SKClientDHTConfiguration(pCdc);
}



