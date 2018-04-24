#include <exception>
#include "jenumutil.h"
#include "SKPutOptions.h"
#include "SKSecondaryTarget.h"
#include "SKOpTimeoutController.h"
#include "SKClientException.h"

using std::endl;
using std::cout;
using std::set;

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/JArray.h"
using jace::JArray;
#include "jace/proxy/types/JByte.h"
using jace::proxy::types::JByte;

#include "jace/proxy/java/util/Set.h"
using jace::proxy::java::util::Set;
#include "jace/proxy/java/util/HashSet.h"
using jace::proxy::java::util::HashSet;
#include "jace/proxy/java/util/Iterator.h"
using jace::proxy::java::util::Iterator;
#include "jace/proxy/java/lang/Throwable.h"
using jace::proxy::java::lang::Throwable;

#include "jace/proxy/com/ms/silverking/cloud/dht/PutOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::PutOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/Compression.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::Compression;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/ChecksumType.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::ChecksumType;
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespacePerspectiveOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespacePerspectiveOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/SecondaryTarget.h"
using jace::proxy::com::ms::silverking::cloud::dht::SecondaryTarget;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/OpTimeoutController.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::OpTimeoutController;
#include "jace/proxy/com/ms/silverking/log/Log.h"
using jace::proxy::com::ms::silverking::log::Log;

typedef JArray< jace::proxy::types::JByte > ByteArray;


SKPutOptions * SKPutOptions::opTimeoutController(SKOpTimeoutController * opTimeoutController)
{
	//OpTimeoutController *controller = java_cast<OpTimeoutController>( *(opTimeoutController->getPImpl()) );
    OpTimeoutController *controller = NULL;// FIXME
	PutOptions * pPutOptImp = new PutOptions(java_cast<PutOptions>(
		((PutOptions*)pImpl)->opTimeoutController(*controller)
	)); 
    delete ((PutOptions*)pImpl);
    pImpl = pPutOptImp;
    return this;
}

SKPutOptions * SKPutOptions::secondaryTargets(set<SKSecondaryTarget*> * secondaryTargets)
{
	Set targets = java_new<HashSet>();
	if(secondaryTargets && secondaryTargets->size()>0) 
	{
		std::set<SKSecondaryTarget*>::iterator it;
		for (it = secondaryTargets->begin(); it != secondaryTargets->end(); ++it)
		{
			SecondaryTarget * pSt = (*it)->getPImpl();
			targets.add(*pSt );
		}
	}

	PutOptions * pPutOptImp = new PutOptions(java_cast<PutOptions>(
		((PutOptions*)pImpl)->secondaryTargets(targets)
	)); 
    delete ((PutOptions*)pImpl);
    pImpl = pPutOptImp;
    return this;
}

SKPutOptions * SKPutOptions::secondaryTargets(SKSecondaryTarget * secondaryTarget)
{
	PutOptions* pPoImp = ((PutOptions*)pImpl);
	PutOptions * pPutOptImp = NULL;
	if(secondaryTarget!=NULL) {
		pPutOptImp = new PutOptions(java_cast<PutOptions>(
			pPoImp->secondaryTargets( *(secondaryTarget->getPImpl()) )
		)); 
	}
	else {
		pPutOptImp = new PutOptions(java_cast<PutOptions>(
			pPoImp->secondaryTargets( SecondaryTarget() )
		)); 
	}
    delete pPoImp;
    pImpl = pPutOptImp;
    return this;
}

SKPutOptions * SKPutOptions::compression(SKCompression::SKCompression compression){
	Compression * pCompr = ::getCompression(compression);
	PutOptions * pPutOptImp = new PutOptions(java_cast<PutOptions>(
		((PutOptions*)pImpl)->compression(*pCompr)
	)); 
    return new SKPutOptions(pPutOptImp);
    /*
	delete pCompr;
    delete ((PutOptions*)pImpl);
    pImpl = pPutOptImp;
    return this;
    */
}

SKPutOptions * SKPutOptions::checksumType(SKChecksumType::SKChecksumType checksumType){
	ChecksumType * pChecksumType = ::getChecksumType(checksumType);
	PutOptions * pPutOptImp = new PutOptions(java_cast<PutOptions>(
		((PutOptions*)pImpl)->checksumType(*pChecksumType)
	)); 
	if(!pPutOptImp || pPutOptImp->isNull()) {
		cout << "Failed to update SKPutOptions" <<endl;
	}
    return new SKPutOptions(pPutOptImp);
    /*
	delete ((NamespacePerspectiveOptions*)pImpl);
	pImpl = pPutOptImp;
	delete pChecksumType;
    return this;
    */
}

SKPutOptions * SKPutOptions::checksumCompressedValues(bool checksumCompressedValues ){
	PutOptions * pPutOptImp = new PutOptions(java_cast<PutOptions>(
		((PutOptions*)pImpl)->checksumCompressedValues(checksumCompressedValues)
	)); 
    delete ((NamespacePerspectiveOptions*)pImpl);
    pImpl = pPutOptImp;
    return this;
}

SKPutOptions * SKPutOptions::version(int64_t version){
	PutOptions * pPutOptImp = new PutOptions(java_cast<PutOptions>(
		((PutOptions*)pImpl)->version(version)
	)); 
    delete ((PutOptions*)pImpl);
    pImpl = pPutOptImp;
    return this;
}

SKPutOptions * SKPutOptions::userData(SKVal * userData){
	const void * value = userData->m_pVal;
	size_t valueLen = userData->m_len;
	ByteArray byteArray(valueLen);
	if(valueLen>0) {
		JNIEnv* env = attach();
		env->SetByteArrayRegion(static_cast<jbyteArray>(byteArray.getJavaJniArray()), 0, valueLen, (const jbyte*)value );
	}

	PutOptions * pPutOptImp = new PutOptions(java_cast<PutOptions>(
		((PutOptions*)pImpl)->userData(byteArray)
	)); 
    delete ((PutOptions*)pImpl);
    pImpl = pPutOptImp;
    return this;
}

////////

SKCompression::SKCompression SKPutOptions::getCompression() const {
	int  compr = (int)((PutOptions*)pImpl)->getCompression().ordinal() ; 
	return static_cast<SKCompression::SKCompression> (compr);
}

SKChecksumType::SKChecksumType SKPutOptions::getChecksumType() const {
	int  chksm = (int)((PutOptions*)pImpl)->getChecksumType().ordinal() ; 
	return static_cast<SKChecksumType::SKChecksumType> (chksm);
}

bool SKPutOptions::getChecksumCompressedValues() const {
	return (bool)((PutOptions*)pImpl)->getChecksumCompressedValues();
}

int64_t SKPutOptions::getVersion() const {
	return (int64_t)((PutOptions*)pImpl)->getVersion() ; 
}

SKVal * SKPutOptions::getUserData() const {
	SKVal * pVal = sk_create_val();

	ByteArray obj = java_cast<ByteArray>(((PutOptions*)pImpl)->getUserData());
	if(obj.isNull()) {
		return pVal;  //empty value
	}
		
	size_t valLength = obj.length();
	if(valLength == 0) {
		return pVal;  //empty value
	}

	JNIEnv* env = attach();
	jbyte * carr = (jbyte *) skMemAlloc(valLength, sizeof(jbyte), __FILE__, __LINE__);
	env->GetByteArrayRegion(static_cast<jbyteArray>(obj.getJavaJniArray()), 0, valLength, carr );
	sk_set_val_zero_copy(pVal, valLength, (void*) carr);
	return pVal;  //non-empty value
}

////////

/* static */
SKPutOptions * SKPutOptions::parse(const char * def){
	PutOptions * pPutOpts = new PutOptions(java_cast<PutOptions>(
			PutOptions::parse(java_new<String>((char*)def))));
	return new SKPutOptions(pPutOpts);
}

string SKPutOptions::toString() const {
	string representation = (string)(((PutOptions*)pImpl)->toString());
	return representation;
}

bool SKPutOptions::equals(SKPutOptions * other) const {
	PutOptions* ppo2 = (PutOptions*)other->pImpl;
	return (bool)((PutOptions*)pImpl)->equals(*ppo2);
}

////////

/* c-tors / d-tors */
SKPutOptions::SKPutOptions(){ pImpl = NULL; };
SKPutOptions::SKPutOptions(void * pPutOptsImpl) : pImpl(pPutOptsImpl) {};  //FIXME ?
void * SKPutOptions::getPImpl() {return pImpl;}  //FIXME:

SKPutOptions::SKPutOptions(SKOpTimeoutController * opTimeoutController, 
        set<SKSecondaryTarget*> * secondaryTargets,
		SKCompression::SKCompression compression, SKChecksumType::SKChecksumType checksumType,
		bool checksumCompressedValues, int64_t version, 
		SKVal * userData)
{
	Compression * pCompr = ::getCompression(compression);
	ChecksumType * pCt = ::getChecksumType(checksumType);
	OpTimeoutController controller = java_cast<OpTimeoutController>( *(opTimeoutController->getPImpl()) );

	Set targets ;
	if(secondaryTargets && secondaryTargets->size()){
		targets = java_new<HashSet>();
		std::set<SKSecondaryTarget*>::iterator it;
		for (it = secondaryTargets->begin(); it != secondaryTargets->end(); ++it)
		{
			SecondaryTarget * pTgt = (*it)->getPImpl();
			targets.add( *pTgt );
		}
	}
	
	const void * value = userData->m_pVal;
	size_t valueLen = userData->m_len;
	ByteArray byteArray(valueLen);
	if(valueLen>0) {
		JNIEnv* env = attach();
		env->SetByteArrayRegion(static_cast<jbyteArray>(byteArray.getJavaJniArray()), 0, valueLen, (const jbyte*)value );
	}
	
	pImpl = new PutOptions(java_new<PutOptions>(controller, targets, *pCompr, *pCt, checksumCompressedValues,
				version, byteArray)); 
	delete pCompr;
	delete pCt;
}

SKPutOptions::~SKPutOptions()
{
	//FIXME: change for inheritance 
	if(pImpl!=NULL) {
		PutOptions * po = (PutOptions*)pImpl;
		delete po; 
		pImpl = NULL;
	}
}
