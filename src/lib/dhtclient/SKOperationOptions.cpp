#include <exception>
#include "jenumutil.h"
#include "SKOperationOptions.h"
#include "SKOpTimeoutController.h"
#include "SKSecondaryTarget.h"
#include "SKClientException.h"
using std::set;

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/proxy/types/JBoolean.h"
using jace::proxy::types::JBoolean;

#include "jace/proxy/java/util/Set.h"
using jace::proxy::java::util::Set;
#include "jace/proxy/java/util/HashSet.h"
using jace::proxy::java::util::HashSet;
#include "jace/proxy/java/util/Iterator.h"
using jace::proxy::java::util::Iterator;
#include "jace/proxy/java/lang/Throwable.h"
using jace::proxy::java::lang::Throwable;
#include "jace/proxy/com/ms/silverking/cloud/dht/OperationOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::OperationOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/OpTimeoutController.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::OpTimeoutController;
#include "jace/proxy/com/ms/silverking/cloud/dht/SecondaryTarget.h"
using jace::proxy::com::ms::silverking::cloud::dht::SecondaryTarget;


////////

SKOpTimeoutController * SKOperationOptions::getOpTimeoutController() {
	OpTimeoutController *  p = new OpTimeoutController( java_cast<OpTimeoutController>( ((OperationOptions*)pImpl)->getOpTimeoutController() )) ; 
	return new SKOpTimeoutController(p);
}

std::set<SKSecondaryTarget*> * SKOperationOptions::getSecondaryTargets()
{
	try {
		Set secondaryTargets = ((OperationOptions*)pImpl)->getSecondaryTargets();
		set<SKSecondaryTarget*> * pTargets = NULL;
		if(secondaryTargets.isNull() || !secondaryTargets.size()){
			Log::fine( "no SecondaryTargets found" );
			return pTargets;
		}

		pTargets = new set<SKSecondaryTarget*>();
		for (Iterator it(secondaryTargets.iterator()); it.hasNext();) 
		{
			SecondaryTarget * entry =  new SecondaryTarget( java_cast<SecondaryTarget>(it.next()) );
			if(!entry->isNull()) {
				SKSecondaryTarget * pSt = new SKSecondaryTarget(entry);
				pTargets->insert(pSt);
			}
		}

		if(pTargets->size() == 0 )
		{
			delete pTargets; pTargets = NULL;
		}
		return pTargets;
	} catch (Throwable &t){
		throw SKClientException( &t, __FILE__, __LINE__ );
	}
}

////////

bool SKOperationOptions::equals(SKOperationOptions * other) const {
	OperationOptions * pro = (OperationOptions *) other->pImpl;
	return  (bool)((OperationOptions*)pImpl)->equals(*pro); 
}

string SKOperationOptions::toString() const{
	string representation = (string)(((OperationOptions*)pImpl)->toString());
	return representation;
}

////////

SKOperationOptions::~SKOperationOptions()
{
	//FIXME: change for inheritance 
	if(pImpl!=NULL) {
		OperationOptions * po = (OperationOptions*)pImpl;
		delete po; 
		pImpl = NULL;
	}
}

SKOperationOptions::SKOperationOptions(SKOpTimeoutController * opTimeoutController,
            std::set<SKSecondaryTarget*> * secondaryTargets)
{
	OpTimeoutController * pTimeoutCtrl = opTimeoutController->getPImpl();
	
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

	pImpl = new OperationOptions(java_new<OperationOptions>( *pTimeoutCtrl, targets )); 
}

SKOperationOptions::SKOperationOptions(void * pOpt) : pImpl(pOpt) {};  //FIXME: make protected ?
//protected:
SKOperationOptions::SKOperationOptions(){ pImpl = NULL; };

