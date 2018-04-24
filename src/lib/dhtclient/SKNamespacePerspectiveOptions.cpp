#include <exception>
#include "jenumutil.h"
#include "SKNamespacePerspectiveOptions.h"
using std::endl;
using std::cout;

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/JArray.h"
using jace::JArray;
#include "jace/proxy/types/JByte.h"
using jace::proxy::types::JByte;

#include "jace/proxy/java/lang/Class.h"
using jace::proxy::java::lang::Class;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/java/lang/Object.h"
using ::jace::proxy::java::lang::Object;
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespaceCreationOptions_Mode.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespaceCreationOptions_Mode;
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespacePerspectiveOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespacePerspectiveOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/PutOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::PutOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/InvalidationOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::InvalidationOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/GetOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::GetOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/WaitOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::WaitOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/KeyDigestType.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::KeyDigestType;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/VersionProvider.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::VersionProvider;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/ConstantVersionProvider.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::ConstantVersionProvider;
typedef JArray< jace::proxy::types::JByte > ByteArray;

SKNamespacePerspectiveOptions::SKNamespacePerspectiveOptions( /* KeyClass k, ValueClass v, */ 
		SKKeyDigestType::SKKeyDigestType keyDigestType, 
		SKPutOptions * defaultPutOpts, 
        SKInvalidationOptions * defaultInvalidationOpts,
        SKGetOptions * defaultGetOpts, 
		SKWaitOptions * defaultWaitOpts, SKVersionProvider * defaultVersionProvider)
{
	ByteArray byteArray(3);
	Class byteArryCls(byteArray.staticGetJavaJniClass().getClass());
	Class strCls(String("").staticGetJavaJniClass().getClass());
	pImpl = new NamespacePerspectiveOptions(java_new<NamespacePerspectiveOptions>(strCls, byteArryCls));  // <String, byte[]>

	KeyDigestType * pKeyDigestType = getDigestType(keyDigestType);
	PutOptions * pPutOptions = (PutOptions *) defaultPutOpts->getPImpl();
	InvalidationOptions * pInvalidationOptions = (InvalidationOptions *) defaultInvalidationOpts->getPImpl();
	GetOptions * pGetOptions = (GetOptions *) defaultGetOpts->getPImpl();
	WaitOptions * pWaitOptions = (WaitOptions *) defaultWaitOpts->getPImpl();
	VersionProvider * pVersionProvider = (VersionProvider *) defaultVersionProvider->getPImpl();
	// <String, byte[]>
	pImpl = new NamespacePerspectiveOptions(java_new<NamespacePerspectiveOptions>(strCls, byteArryCls, 
		*pKeyDigestType, *pPutOptions, *pInvalidationOptions, *pGetOptions, *pWaitOptions, *pVersionProvider)); 
	delete pKeyDigestType;
}

SKNamespacePerspectiveOptions::SKNamespacePerspectiveOptions(void * pOpt) : pImpl(pOpt) {};

SKNamespacePerspectiveOptions::~SKNamespacePerspectiveOptions()
{
	NamespacePerspectiveOptions * po = (NamespacePerspectiveOptions*)pImpl;
    delete po; 
	pImpl = NULL;
}

////////

SKNamespacePerspectiveOptions * SKNamespacePerspectiveOptions::keyDigestType(SKKeyDigestType::SKKeyDigestType keyDigestType){
	KeyDigestType * pKeyDigestType = getDigestType(keyDigestType);
	NamespacePerspectiveOptions * pNspoImp = new NamespacePerspectiveOptions(java_cast<NamespacePerspectiveOptions>(
		((NamespacePerspectiveOptions*)pImpl)->keyDigestType(*pKeyDigestType)
	)); 
    return new SKNamespacePerspectiveOptions(pNspoImp);
    /*
	delete pKeyDigestType;
    delete ((NamespacePerspectiveOptions*)pImpl);
    pImpl = pNspoImp;
    return this;
    */
}

SKNamespacePerspectiveOptions * SKNamespacePerspectiveOptions::defaultPutOptions(SKPutOptions * defaultPutOptions){
	PutOptions * pPutOptions = (PutOptions *) defaultPutOptions->getPImpl();
	NamespacePerspectiveOptions * pNspoImp = new NamespacePerspectiveOptions(java_cast<NamespacePerspectiveOptions>(
		((NamespacePerspectiveOptions*)pImpl)->defaultPutOptions(*pPutOptions)
	)); 
    return new SKNamespacePerspectiveOptions(pNspoImp);
    /*
    delete ((NamespacePerspectiveOptions*)pImpl);
    pImpl = pNspoImp;
    return this;
    */
}

SKNamespacePerspectiveOptions * SKNamespacePerspectiveOptions::defaultInvalidationOptions(SKInvalidationOptions * defaultInvalidationOptions){
	InvalidationOptions * pInvalidationOptions = (InvalidationOptions *) defaultInvalidationOptions->getPImpl();
	NamespacePerspectiveOptions * pNspoImp = new NamespacePerspectiveOptions(java_cast<NamespacePerspectiveOptions>(
		((NamespacePerspectiveOptions*)pImpl)->defaultInvalidationOptions(*pInvalidationOptions)
	)); 
    return new SKNamespacePerspectiveOptions(pNspoImp);
    /*
    delete ((NamespacePerspectiveOptions*)pImpl);
    pImpl = pNspoImp;
    return this;
    */
}

SKNamespacePerspectiveOptions * SKNamespacePerspectiveOptions::defaultGetOptions(SKGetOptions * defaultGetOptions){
	GetOptions * pGetOptions = (GetOptions *) defaultGetOptions->getPImpl();
	NamespacePerspectiveOptions * pNspoImp = new NamespacePerspectiveOptions(java_cast<NamespacePerspectiveOptions>(
		((NamespacePerspectiveOptions*)pImpl)->defaultGetOptions(*pGetOptions)
	)); 
    return new SKNamespacePerspectiveOptions(pNspoImp);
    /*
    delete ((NamespacePerspectiveOptions*)pImpl);
    pImpl = pNspoImp;
    return this;
    */
}

SKNamespacePerspectiveOptions * SKNamespacePerspectiveOptions::defaultWaitOptions(SKWaitOptions * defaultWaitOptions){
	WaitOptions * pWaitOptions = (WaitOptions *) defaultWaitOptions->getPImpl();
	NamespacePerspectiveOptions * pNspoImp = new NamespacePerspectiveOptions(java_cast<NamespacePerspectiveOptions>(
		((NamespacePerspectiveOptions*)pImpl)->defaultWaitOptions(*pWaitOptions)
	)); 
    return new SKNamespacePerspectiveOptions(pNspoImp);
    /*
    delete ((NamespacePerspectiveOptions*)pImpl);
    pImpl = pNspoImp;
    return this;
    */
}

SKNamespacePerspectiveOptions * SKNamespacePerspectiveOptions::defaultVersionProvider(SKVersionProvider * defaultVersionProvider){
	VersionProvider * pVersionProvider = (VersionProvider *) defaultVersionProvider->getPImpl();
	NamespacePerspectiveOptions * pNspoImp = new NamespacePerspectiveOptions(java_cast<NamespacePerspectiveOptions>(
		((NamespacePerspectiveOptions*)pImpl)->defaultVersionProvider(*pVersionProvider)
	)); 
    delete ((NamespacePerspectiveOptions*)pImpl);
    pImpl = pNspoImp;
    return this;
}

////////

SKKeyDigestType::SKKeyDigestType SKNamespacePerspectiveOptions::getKeyDigestType(){
	int  keyDigestType = (int)((NamespacePerspectiveOptions*)pImpl)->getKeyDigestType().ordinal() ; 
	return static_cast<SKKeyDigestType::SKKeyDigestType> (keyDigestType);
}

SKPutOptions * SKNamespacePerspectiveOptions::getDefaultPutOptions(){
	PutOptions * pPutOptsImpl = new PutOptions(java_cast<PutOptions>(
		((NamespacePerspectiveOptions*)pImpl)->getDefaultPutOptions()
	)); 
    //bool isnul = pPutOptsImpl->isNull();
    //cout << "Default PutOptions "  << isnul <<endl;
	return new SKPutOptions(pPutOptsImpl);
	
}

SKInvalidationOptions * SKNamespacePerspectiveOptions::getDefaultInvalidationOptions(){
	InvalidationOptions * p = new InvalidationOptions(java_cast<InvalidationOptions>(
		((NamespacePerspectiveOptions*)pImpl)->getDefaultInvalidationOptions()
	)); 
    //bool isnul = p->isNull();
    //cout << "Default InvalidationOptions "  << isnul <<endl;
	return new SKInvalidationOptions(p);
	
}

SKGetOptions * SKNamespacePerspectiveOptions::getDefaultGetOptions(){
	GetOptions * pGetOptsImpl = new GetOptions(java_cast<GetOptions>(
		((NamespacePerspectiveOptions*)pImpl)->getDefaultGetOptions()
	)); 
	return new SKGetOptions(pGetOptsImpl);
}

SKWaitOptions * SKNamespacePerspectiveOptions::getDefaultWaitOptions(){
	WaitOptions * pWaitOptsImpl = new WaitOptions(java_cast<WaitOptions>(
		((NamespacePerspectiveOptions*)pImpl)->getDefaultWaitOptions()
	)); 
	return new SKWaitOptions(pWaitOptsImpl);
}

SKVersionProvider * SKNamespacePerspectiveOptions::getDefaultVersionProvider(){
	VersionProvider * pVersionProviderImpl = new VersionProvider(java_cast<VersionProvider>(
		((NamespacePerspectiveOptions*)pImpl)->getDefaultVersionProvider()
	)); 
	return new SKVersionProvider(pVersionProviderImpl);
}

////////

void * SKNamespacePerspectiveOptions::getPImpl(){
	return pImpl;
}

string SKNamespacePerspectiveOptions::toString(){
	string representation = (string)(((NamespacePerspectiveOptions*)pImpl)->toString());
	return representation;
}

SKNamespacePerspectiveOptions * SKNamespacePerspectiveOptions::parse(const char * def) {
	NamespacePerspectiveOptions * pNspoImp = new NamespacePerspectiveOptions(java_cast<NamespacePerspectiveOptions>(
		((NamespacePerspectiveOptions*)pImpl)->parse(java_new<String>((char *)def))
	)); 
    delete ((NamespacePerspectiveOptions*)pImpl);
    pImpl = pNspoImp;
    return this;
}
