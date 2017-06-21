#include <exception>
#include "jenumutil.h"
#include "SKInvalidationOptions.h"
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

#include "jace/proxy/com/ms/silverking/cloud/dht/InvalidationOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::InvalidationOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespacePerspectiveOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespacePerspectiveOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/SecondaryTarget.h"
using jace::proxy::com::ms::silverking::cloud::dht::SecondaryTarget;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/OpTimeoutController.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::OpTimeoutController;
#include "jace/proxy/com/ms/silverking/log/Log.h"
using jace::proxy::com::ms::silverking::log::Log;

typedef JArray< jace::proxy::types::JByte > ByteArray;


SKInvalidationOptions * SKInvalidationOptions::opTimeoutController(SKOpTimeoutController * opTimeoutController)
{
	//OpTimeoutController *controller = java_cast<OpTimeoutController>( *(opTimeoutController->getPImpl()) );
    OpTimeoutController *controller = NULL; // FIXME
	InvalidationOptions * p = new InvalidationOptions(java_cast<InvalidationOptions>(
		((InvalidationOptions*)pImpl)->opTimeoutController(*controller)
	)); 
    delete ((InvalidationOptions*)pImpl);
    pImpl = p;
    return this;
}

SKInvalidationOptions * SKInvalidationOptions::secondaryTargets(set<SKSecondaryTarget*> * secondaryTargets) {
	Set targets = java_new<HashSet>();
	if (secondaryTargets && secondaryTargets->size() > 0) {
		std::set<SKSecondaryTarget*>::iterator it;
		for (it = secondaryTargets->begin(); it != secondaryTargets->end(); ++it) {
			SecondaryTarget * pSt = (*it)->getPImpl();
			targets.add(*pSt );
		}
	}

	InvalidationOptions * p = new InvalidationOptions(java_cast<InvalidationOptions>(
		((InvalidationOptions*)pImpl)->secondaryTargets(targets)
	)); 
    delete ((InvalidationOptions*)pImpl);
    pImpl = p;
    return this;
}

SKInvalidationOptions * SKInvalidationOptions::secondaryTargets(SKSecondaryTarget * secondaryTarget) {
	InvalidationOptions* pPoImp = ((InvalidationOptions*)pImpl);
	InvalidationOptions * p = NULL;
	if(secondaryTarget!=NULL) {
		p = new InvalidationOptions(java_cast<InvalidationOptions>(
			pPoImp->secondaryTargets( *(secondaryTarget->getPImpl()) )
		)); 
	} else {
		p = new InvalidationOptions(java_cast<InvalidationOptions>(
			pPoImp->secondaryTargets( SecondaryTarget() )
		)); 
	}
    delete pPoImp;
    pImpl = p;
    return this;
}

SKInvalidationOptions * SKInvalidationOptions::version(int64_t version){
	InvalidationOptions * p = new InvalidationOptions(java_cast<InvalidationOptions>(
		((InvalidationOptions*)pImpl)->version(version)
	)); 
    delete ((InvalidationOptions*)pImpl);
    pImpl = p;
    return this;
}

////////

/* static */
SKInvalidationOptions * SKInvalidationOptions::parse(const char * def) {
	InvalidationOptions * pPutOpts = new InvalidationOptions(java_cast<InvalidationOptions>(
			InvalidationOptions::parse(java_new<String>((char*)def))));
	return new SKInvalidationOptions(pPutOpts);
}

string SKInvalidationOptions::toString() const {
	string representation = (string)(((InvalidationOptions*)pImpl)->toString());
	return representation;
}

bool SKInvalidationOptions::equals(SKInvalidationOptions * other) const {
	InvalidationOptions *ppo2 = (InvalidationOptions*)other->pImpl;
	return (bool)((InvalidationOptions*)pImpl)->equals(*ppo2);
}

////////

/* c-tors / d-tors */
SKInvalidationOptions::SKInvalidationOptions(void * pPutOptsImpl) : pImpl(pPutOptsImpl) {};  //FIXME ?
void * SKInvalidationOptions::getPImpl() {return pImpl;}  //FIXME:

SKInvalidationOptions::SKInvalidationOptions(SKOpTimeoutController * opTimeoutController, 
        set<SKSecondaryTarget*> * secondaryTargets, int64_t version) {
	OpTimeoutController controller = java_cast<OpTimeoutController>( *(opTimeoutController->getPImpl()) );

	Set targets ;
	if (secondaryTargets && secondaryTargets->size()) {
		targets = java_new<HashSet>();
		std::set<SKSecondaryTarget*>::iterator it;
		for (it = secondaryTargets->begin(); it != secondaryTargets->end(); ++it) {
			SecondaryTarget * pTgt = (*it)->getPImpl();
			targets.add( *pTgt );
		}
	}
	
	pImpl = new InvalidationOptions(java_new<InvalidationOptions>(controller, 
                targets, version)); 
}

SKInvalidationOptions::~SKInvalidationOptions() {
	//FIXME: change for inheritance 
	if (pImpl != NULL) {
		InvalidationOptions *io = (InvalidationOptions*)pImpl;
		delete io; 
		pImpl = NULL;
	}
}
