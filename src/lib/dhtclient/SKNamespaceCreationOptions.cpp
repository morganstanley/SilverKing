#include "SKNamespaceCreationOptions.h"
#include "SKNamespaceOptions.h"
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
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespaceCreationOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespaceCreationOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespaceCreationOptions_Mode.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespaceCreationOptions_Mode;
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespaceOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespaceOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/StorageType.h"
using jace::proxy::com::ms::silverking::cloud::dht::StorageType;
#include "jace/proxy/com/ms/silverking/cloud/dht/ConsistencyProtocol.h"
using jace::proxy::com::ms::silverking::cloud::dht::ConsistencyProtocol;
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespaceVersionMode.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespaceVersionMode;
#include "jace/proxy/com/ms/silverking/cloud/dht/PutOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::PutOptions;


/* static */
NamespaceCreationOptions_Mode * getNsCreationMode(NsCreationMode mode) {
    switch(mode)
    {
        case RequireExplicitCreation: 
            return new NamespaceCreationOptions_Mode (NamespaceCreationOptions_Mode::valueOf("RequireExplicitCreation"));
        case RequireAutoCreation: 
            return new NamespaceCreationOptions_Mode (NamespaceCreationOptions_Mode::valueOf("RequireAutoCreation"));
        case OptionalAutoCreation_AllowMatches: 
            return new NamespaceCreationOptions_Mode (NamespaceCreationOptions_Mode::valueOf("OptionalAutoCreation_AllowMatches"));
        case OptionalAutoCreation_DisallowMatches: 
            return new NamespaceCreationOptions_Mode (NamespaceCreationOptions_Mode::valueOf("OptionalAutoCreation_DisallowMatches"));
        default: 
            throw std::exception(); //FIXME:
    }
}

SKNamespaceCreationOptions * SKNamespaceCreationOptions::parse(const char * def){
    NamespaceCreationOptions * pNSOpts = new NamespaceCreationOptions(java_cast<NamespaceCreationOptions>(
            NamespaceCreationOptions::parse(java_new<String>((char *)def))));
    return new SKNamespaceCreationOptions(pNSOpts);
}

SKNamespaceCreationOptions * SKNamespaceCreationOptions::defaultOptions() {
    NamespaceCreationOptions * pNSOpts = new NamespaceCreationOptions(java_cast<NamespaceCreationOptions>(
            NamespaceCreationOptions::defaultOptions() ));
    return new SKNamespaceCreationOptions(pNSOpts);
}

/* ctors / dtors */
SKNamespaceCreationOptions::SKNamespaceCreationOptions(NsCreationMode mode, const char * regex, SKNamespaceOptions * defaultNSOptions){

    NamespaceCreationOptions_Mode * pNCOMode = getNsCreationMode(mode);
    NamespaceOptions * pNo = (NamespaceOptions *) defaultNSOptions->getPImpl();  //FIXME: friend
    pImpl = new NamespaceCreationOptions(java_new<NamespaceCreationOptions>(*pNCOMode, java_new<String>((char *)regex), *pNo)); 
    delete pNCOMode;
}

SKNamespaceCreationOptions::SKNamespaceCreationOptions(void * pNamespaceCreationOptions)
    : pImpl(pNamespaceCreationOptions) {}; 
    
SKNamespaceCreationOptions::~SKNamespaceCreationOptions()
{
    if(pImpl!=NULL) {
        NamespaceCreationOptions * pNSOpts = (NamespaceCreationOptions*)pImpl;
        delete pNSOpts; 
        pImpl = NULL;
    }
}

void * SKNamespaceCreationOptions::getPImpl(){
    return pImpl;
}


/* public methods */
bool SKNamespaceCreationOptions::canBeExplicitlyCreated(const char * ns){
    return (bool)((NamespaceCreationOptions*)pImpl)->canBeExplicitlyCreated(java_new<String>((char *)ns)); 
}

bool SKNamespaceCreationOptions::canBeAutoCreated(const char * ns){
    return (bool)((NamespaceCreationOptions*)pImpl)->canBeAutoCreated(java_new<String>((char *)ns)); 
}

SKNamespaceOptions * SKNamespaceCreationOptions::getDefaultNamespaceOptions() {
    NamespaceOptions * pNamespaceOptions = new NamespaceOptions( java_cast<NamespaceOptions>(
                    ((NamespaceCreationOptions*)pImpl)->getDefaultNamespaceOptions())); 
    return new SKNamespaceOptions(pNamespaceOptions);
}

char * SKNamespaceCreationOptions::toString() const {
    string representation = (string)((NamespaceCreationOptions*)pImpl)->toString(); 
    return skStrDup(representation.c_str(),__FILE__, __LINE__);
    
}

