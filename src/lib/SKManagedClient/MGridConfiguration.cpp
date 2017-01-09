#include "StdAfx.h"
#include "MGridConfiguration.h"
#include "MClientDHTConfiguration.h"

#include <string>
#include "SKGridConfiguration.h"
#include "SKClientDHTConfigurationProvider.h"
#include "SKClientDHTConfiguration.h"

using namespace std;

using namespace System::Runtime::InteropServices;

namespace SKManagedClient {

MGridConfiguration::!MGridConfiguration(){
	if(pImpl)
	{
		delete (SKGridConfiguration*) pImpl; 
		pImpl = NULL;
	}
}

MGridConfiguration::~MGridConfiguration()
{ 
	this->!MGridConfiguration(); 
} 

MGridConfiguration::MGridConfiguration(SKGridConfiguration_M ^ gridConfImpl) {
	pImpl = gridConfImpl->pGridConfig;
}

SKGridConfiguration_M ^ MGridConfiguration::getPImpl() {
	SKGridConfiguration_M ^ gc = gcnew SKGridConfiguration_M ;
	gc->pGridConfig = pImpl;
	return gc;
}

MGridConfiguration::MGridConfiguration(System::String ^ name, Dictionary<String ^ , String ^ > ^ envMap){
	pImpl  = NULL;
	char * pName = NULL;
	std::map<string, string> * pEnvMap = NULL;
	try {
		pEnvMap =  new  std::map<string, string>();
		pName = (char*)(void*)Marshal::StringToHGlobalAnsi(name);

		IDictionaryEnumerator ^ iter = envMap->GetEnumerator();
		while (iter->MoveNext())
        {
			pEnvMap->insert( std::map<string, string>::value_type (
				std::string( (char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(iter->Key->ToString()).ToPointer()),
				std::string( (char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(iter->Value->ToString()).ToPointer()) 
			));

		}

		pImpl = new SKGridConfiguration(pName, pEnvMap);
    }
    finally
    {
        Marshal::FreeHGlobal(System::IntPtr(pName));
		delete pEnvMap;
    }
}

MGridConfiguration ^ MGridConfiguration::parseFile(System::String ^ gcBase, System::String ^ gcName){

	SKGridConfiguration * pGridConfig =  SKGridConfiguration::parseFile(
		(char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(gcBase).ToPointer(),
		(char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(gcName).ToPointer()
	);
	SKGridConfiguration_M ^ gc = gcnew SKGridConfiguration_M ;
	gc->pGridConfig = pGridConfig;
	return gcnew MGridConfiguration(gc);
}

MGridConfiguration ^ MGridConfiguration::parseFile(System::String ^ gcName){
	SKGridConfiguration * pGridConfig =  SKGridConfiguration::parseFile(
		(char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(gcName).ToPointer() );
	SKGridConfiguration_M ^ gc = gcnew SKGridConfiguration_M ;
	gc->pGridConfig = pGridConfig;
	return gcnew MGridConfiguration(gc);
}

Dictionary<String ^ , String ^ >  ^ MGridConfiguration::readEnvFile(System::String ^ envFile){

	std::map<string, string> * pEnvMap = SKGridConfiguration::readEnvFile(
		(char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(envFile).ToPointer()
	);

	Dictionary<String ^ , String ^ >  ^ envMap = nullptr;
	if(pEnvMap)
	{
		envMap = gcnew Dictionary<String ^ , String ^ > ();
		std::map<string, string>::iterator it = pEnvMap->begin();
		for (it ; it!=pEnvMap->end(); it++){
			envMap->Add(gcnew System::String(it->first.c_str()), gcnew System::String(it->first.c_str()) );
		}
		pEnvMap->clear();
		delete pEnvMap;
	}

	return envMap;
}

System::String ^ MGridConfiguration::getName() {
	char * pName = ((SKGridConfiguration*)pImpl)->getName();
	System::String ^ envVal = gcnew System::String(pName);
	free( pName );
	return envVal;
}
System::String ^ MGridConfiguration::get(System::String ^ envKey){
	char * pEnvVal = ((SKGridConfiguration*)pImpl)->get(
			(char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(envKey).ToPointer()
		);
	System::String ^ envVal = gcnew System::String(pEnvVal);
	free( pEnvVal );
	return envVal;
}

System::String ^ MGridConfiguration::toString() {
	char * pEnvVal = ((SKGridConfiguration*)pImpl)->toString();
	System::String ^ envVal = gcnew System::String(pEnvVal);
	free( pEnvVal );
	return envVal;
} 

Dictionary<String ^ , String ^ > ^ MGridConfiguration::getEnvMap() {
	std::map<string,string> * pEnvMap = ((SKGridConfiguration*)pImpl)->getEnvMap();
	Dictionary<String ^ , String ^ > ^ envMap = nullptr;
	if ( pEnvMap ) {
		envMap = gcnew Dictionary<String ^ , String ^ >();
		std::map<string, string>::iterator it = pEnvMap->begin();
		for (it ; it!=pEnvMap->end(); it++){
			envMap->Add(gcnew System::String(it->first.c_str()), gcnew System::String(it->first.c_str()) );
		}
		pEnvMap->clear();
		delete pEnvMap;
	}

	return envMap;
}

MClientDHTConfiguration ^ MGridConfiguration::getClientDHTConfiguration(){
	SKClientDHTConfiguration * pDhtConf =  ((SKGridConfiguration*)pImpl)->getClientDHTConfiguration();
	SKClientDHTConfiguration_M ^ dhtConf = gcnew SKClientDHTConfiguration_M;
	dhtConf->pDhtConfig = pDhtConf;
	return gcnew MClientDHTConfiguration(dhtConf);
}

}
