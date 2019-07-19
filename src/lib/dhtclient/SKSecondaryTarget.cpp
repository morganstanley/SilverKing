#include "skbasictypes.h"
#include "SKSecondaryTarget.h"
#include <string>
using std::string;

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/com/ms/silverking/cloud/dht/SecondaryTarget.h"
using jace::proxy::com::ms::silverking::cloud::dht::SecondaryTarget;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/SecondaryTargetType.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SecondaryTargetType;


SecondaryTargetType * getTargetType(SKSecondaryTargetType type){
    switch(type)
    {
        case NodeID: 
            return new SecondaryTargetType (SecondaryTargetType::valueOf("NodeID"));
        case AncestorClass: 
            return new SecondaryTargetType (SecondaryTargetType::valueOf("AncestorClass"));
        default: 
            throw std::exception(); //FIXME:
    }
}

SKSecondaryTarget::SKSecondaryTarget(SKSecondaryTargetType type, const char * target) 
{
    SecondaryTargetType * pTargetType = getTargetType(type);
    pImpl = new SecondaryTarget(java_new<SecondaryTarget>(*pTargetType, 
                            java_new<String>((char*)target))); 
    delete pTargetType;
}

SKSecondaryTarget::SKSecondaryTarget(SKSecondaryTargetType type, string target){
    SecondaryTargetType * pTargetType = getTargetType(type);
    pImpl = new SecondaryTarget(java_new<SecondaryTarget>(*pTargetType, 
                            java_new<String>((char*) target.c_str()))); 
    delete pTargetType;
}

SKSecondaryTarget::SKSecondaryTarget(SecondaryTarget * impl){
    pImpl = impl;
}

SKSecondaryTarget::~SKSecondaryTarget()
{
    if(pImpl!=NULL) {
        delete pImpl; 
        pImpl = NULL;
    }
}

SecondaryTarget * SKSecondaryTarget::getPImpl(){
    return pImpl;
}

char * SKSecondaryTarget::toString(){
    string representation = (string)(pImpl->toString());
    return skStrDup(representation.c_str(),__FILE__, __LINE__);
}

char * SKSecondaryTarget::getTarget(){
    string target = (string) (pImpl->getTarget());
    return skStrDup(target.c_str(),__FILE__, __LINE__);
}

SKSecondaryTargetType SKSecondaryTarget::getType(){
    int type = (int)(pImpl->getType().ordinal());
    return static_cast<SKSecondaryTargetType>( type );
} 

