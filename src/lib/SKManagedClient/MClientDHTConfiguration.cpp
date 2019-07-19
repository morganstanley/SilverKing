#include "StdAfx.h"
#include <string>
#include <map>
using namespace std;

#include "MClientDHTConfiguration.h"
#include "SKClientDHTConfiguration.h"

using namespace System::Runtime::InteropServices;

namespace SKManagedClient {

MClientDHTConfiguration::!MClientDHTConfiguration(){
    if(pImpl) {
        delete (SKClientDHTConfiguration*) pImpl; 
        pImpl = NULL;
    }
}

MClientDHTConfiguration::MClientDHTConfiguration(SKClientDHTConfiguration_M ^ pClientDHTConfiguration) {
    pImpl = pClientDHTConfiguration->pDhtConfig ;
}

SKClientDHTConfiguration_M ^ MClientDHTConfiguration::getPImpl() {
    SKClientDHTConfiguration_M ^ dhtConf = gcnew SKClientDHTConfiguration_M ;
    dhtConf->pDhtConfig = pImpl;
    return dhtConf;
}


MClientDHTConfiguration ^ MClientDHTConfiguration::create(Dictionary<String^,String^> ^ envMap){
    
    map<string,string> envVars;
    for each( KeyValuePair<String^, String^> kvp in envMap ) 
    {
        char * key = (char*)(void*)Marshal::StringToHGlobalAnsi(kvp.Key);
        char * val = (char*)(void*)Marshal::StringToHGlobalAnsi(kvp.Value);
        envVars.insert(map<string,string>::value_type(string(key), string(val) ));
        Marshal::FreeHGlobal(System::IntPtr(key ));
        Marshal::FreeHGlobal(System::IntPtr(val ));
    }
    SKClientDHTConfiguration * pDhtConf = SKClientDHTConfiguration::create(&envVars);
    SKClientDHTConfiguration_M ^ dhtConfImp = gcnew SKClientDHTConfiguration_M;
    dhtConfImp->pDhtConfig = pDhtConf;
    return gcnew MClientDHTConfiguration(dhtConfImp);
}

MClientDHTConfiguration::MClientDHTConfiguration(String ^ dhtName, String ^ zkLocs){
    char * pName = NULL;
    char * pZkLoc = NULL;
    try {
        pName = (char*)(void*)Marshal::StringToHGlobalAnsi(dhtName);
        pZkLoc = (char*)(void*)Marshal::StringToHGlobalAnsi(zkLocs);
        pImpl = new SKClientDHTConfiguration (pName, pZkLoc);
    }
    finally{
        Marshal::FreeHGlobal(System::IntPtr(pName ));
        Marshal::FreeHGlobal(System::IntPtr(pZkLoc ));
    }
}

MClientDHTConfiguration::MClientDHTConfiguration(String ^ dhtName, int dhtPort, String ^ zkLocs) {
    char * pName = NULL;
    char * pZkLoc = NULL;
    try {
        pName = (char*)(void*)Marshal::StringToHGlobalAnsi(dhtName);
        pZkLoc = (char*)(void*)Marshal::StringToHGlobalAnsi(zkLocs);
        pImpl = new SKClientDHTConfiguration (pName, dhtPort, pZkLoc);
    }
    finally{
        Marshal::FreeHGlobal(System::IntPtr(pName ));
        Marshal::FreeHGlobal(System::IntPtr(pZkLoc ));
    }
}

String ^ MClientDHTConfiguration::getName(){
    char * pName = ((SKClientDHTConfiguration*)pImpl)->toString();
    String ^ name = gcnew String(pName);
    free( pName );
    return name;

}

int MClientDHTConfiguration::getPort() {
    return ((SKClientDHTConfiguration*)pImpl)->getPort();

}

String ^ MClientDHTConfiguration::toString() {
    char * str = ((SKClientDHTConfiguration*)pImpl)->toString();
    String ^ strStr = gcnew String(str);
    free( str );
    return strStr;
}

bool MClientDHTConfiguration::hasPort() 
{
    bool hasPort = ((SKClientDHTConfiguration*)pImpl)->hasPort();
    return hasPort;
}

MClientDHTConfiguration ^ MClientDHTConfiguration::getClientDHTConfiguration(){
    SKClientDHTConfiguration * pConf = ((SKClientDHTConfiguration*)pImpl)->getClientDHTConfiguration();
    SKClientDHTConfiguration_M ^ conf = gcnew SKClientDHTConfiguration_M;
    conf->pDhtConfig = (void *) pConf;
    return gcnew MClientDHTConfiguration( conf );
}


/*
String ^ MClientDHTConfiguration::getZkLocs()   //should be deallocated with delete[]
{
    SKAddrAndPort * pZkLocs = ((SKClientDHTConfiguration*)pImpl)->getZkLocs();
    //TODO: convert this to String^ ?
}
*/


}

