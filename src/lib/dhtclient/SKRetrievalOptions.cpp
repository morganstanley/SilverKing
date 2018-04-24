#include <exception>
#include "jenumutil.h"
#include "SKRetrievalOptions.h"
#include "SKVersionConstraint.h"
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
#include "jace/proxy/com/ms/silverking/cloud/dht/NonExistenceResponse.h"
using jace::proxy::com::ms::silverking::cloud::dht::NonExistenceResponse;
#include "jace/proxy/com/ms/silverking/cloud/dht/RetrievalType.h"
using jace::proxy::com::ms::silverking::cloud::dht::RetrievalType;
#include "jace/proxy/com/ms/silverking/cloud/dht/RetrievalOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::RetrievalOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/VersionConstraint.h"
using jace::proxy::com::ms::silverking::cloud::dht::VersionConstraint;
#include "jace/proxy/com/ms/silverking/cloud/dht/VersionConstraint_Mode.h"
using jace::proxy::com::ms::silverking::cloud::dht::VersionConstraint_Mode;
#include "jace/proxy/com/ms/silverking/cloud/dht/WaitMode.h"
using jace::proxy::com::ms::silverking::cloud::dht::WaitMode;
#include "jace/proxy/com/ms/silverking/cloud/dht/WaitOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::WaitOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/OpTimeoutController.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::OpTimeoutController;
#include "jace/proxy/com/ms/silverking/cloud/dht/net/ForwardingMode.h"
using jace::proxy::com::ms::silverking::cloud::dht::net::ForwardingMode;
#include "jace/proxy/com/ms/silverking/cloud/dht/SecondaryTarget.h"
using jace::proxy::com::ms::silverking::cloud::dht::SecondaryTarget;


////////

SKRetrievalOptions * SKRetrievalOptions::opTimeoutController(SKOpTimeoutController *opTimeoutController){
/*
	//OpTimeoutController * pOpTimeoutController = ::getOpTimeoutController(opTimeoutController);
	OpTimeoutController *pOpTimeoutController = java_cast<OpTimeoutController>( *(opTimeoutController->getPImpl()) );
	RetrievalOptions * pRetrOptImp = new RetrievalOptions(java_cast<RetrievalOptions>(
		((RetrievalOptions*)pImpl)->opTimeoutController(*pOpTimeoutController)
	)); 
	delete pOpTimeoutController;
    delete ((RetrievalOptions*)pImpl);
    pImpl = pRetrOptImp;
    return this;
    */
    return NULL;
}

SKRetrievalOptions * SKRetrievalOptions::secondaryTargets(std::set<SKSecondaryTarget*> * secondaryTargets)
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

	RetrievalOptions * pRetrOptImp = new RetrievalOptions(java_cast<RetrievalOptions>(
		((RetrievalOptions*)pImpl)->secondaryTargets(targets)
	)); 
    delete ((RetrievalOptions*)pImpl);
    pImpl = pRetrOptImp;
    return this;
}

SKRetrievalOptions * SKRetrievalOptions::retrievalType(SKRetrievalType retrievalType){
	RetrievalType * pRetrievalType = ::getRetrievalType(retrievalType);
	RetrievalOptions * pRetrOptImp = new RetrievalOptions(java_cast<RetrievalOptions>(
		((RetrievalOptions*)pImpl)->retrievalType(*pRetrievalType)
	)); 
	delete pRetrievalType;
    delete ((RetrievalOptions*)pImpl);
    pImpl = pRetrOptImp;
    return this;
}

SKRetrievalOptions * SKRetrievalOptions::waitMode(SKWaitMode waitMode){
	WaitMode * pWaitMode = ::getWaitMode(waitMode);
	RetrievalOptions * pRetrOptImp = new RetrievalOptions(java_cast<RetrievalOptions>(
		((RetrievalOptions*)pImpl)->waitMode(*pWaitMode)
	)); 
	delete pWaitMode;
    delete ((RetrievalOptions*)pImpl);
    pImpl = pRetrOptImp;
    return this;
}

SKRetrievalOptions * SKRetrievalOptions::versionConstraint(SKVersionConstraint * versionConstraint){
	VersionConstraint * pvc = (VersionConstraint *) versionConstraint->getPImpl();  //FIXME: friend
	RetrievalOptions * pRetrOptImp = new RetrievalOptions(java_cast<RetrievalOptions>(
		((RetrievalOptions*)pImpl)->versionConstraint(*pvc)
	)); 
    delete ((RetrievalOptions*)pImpl);
    pImpl = pRetrOptImp;
    return this;
}

SKRetrievalOptions * SKRetrievalOptions::nonExistenceResponse(SKNonExistenceResponse::SKNonExistenceResponse nonExistenceResponse){
	NonExistenceResponse * pNer = ::getNonExistenceResponseType(nonExistenceResponse);
	RetrievalOptions * pRetrOptImp = new RetrievalOptions(java_cast<RetrievalOptions>(
		((RetrievalOptions*)pImpl)->nonExistenceResponse(*pNer)
	)); 
	delete pNer;
    delete ((RetrievalOptions*)pImpl);
    pImpl = pRetrOptImp;
    return this;
}

SKRetrievalOptions * SKRetrievalOptions::verifyChecksums(bool verifyChecksums)
{
	RetrievalOptions * pRetrOptImp = new RetrievalOptions(java_cast<RetrievalOptions>(
		((RetrievalOptions*)pImpl)->verifyChecksums(JBoolean(verifyChecksums))
	)); 
    delete ((RetrievalOptions*)pImpl);
    pImpl = pRetrOptImp;
    return this;
}

SKRetrievalOptions * SKRetrievalOptions::returnInvalidations(bool returnInvalidations)
{
	RetrievalOptions * pRetrOptImp = new RetrievalOptions(java_cast<RetrievalOptions>(
		((RetrievalOptions*)pImpl)->returnInvalidations(JBoolean(returnInvalidations))
	)); 
    delete ((RetrievalOptions*)pImpl);
    pImpl = pRetrOptImp;
    return this;
}

SKRetrievalOptions * SKRetrievalOptions::forwardingMode(SKForwardingMode forwardingMode)
{
	ForwardingMode * pFm = ::getForwardingMode(forwardingMode);
	RetrievalOptions * pRetrOptImp = new RetrievalOptions(java_cast<RetrievalOptions>(
		((RetrievalOptions*)pImpl)->forwardingMode(*pFm)
	)); 
	delete pFm;
    delete ((RetrievalOptions*)pImpl);
    pImpl = pRetrOptImp;
    return this;
}

SKRetrievalOptions * SKRetrievalOptions::updateSecondariesOnMiss(bool updateSecondariesOnMiss)
{
	RetrievalOptions * pRetrOptImp = new RetrievalOptions(java_cast<RetrievalOptions>(
		((RetrievalOptions*)pImpl)->updateSecondariesOnMiss(JBoolean(updateSecondariesOnMiss))
	)); 
    delete ((RetrievalOptions*)pImpl);
    pImpl = pRetrOptImp;
    return this;
}

////////

SKRetrievalType SKRetrievalOptions::getRetrievalType() const {
	int  retrievalType = (int)((RetrievalOptions*)pImpl)->getRetrievalType().ordinal() ; 
	return static_cast<SKRetrievalType> (retrievalType);
}

SKWaitMode SKRetrievalOptions::getWaitMode() const {
	int  waitMode = (int)((RetrievalOptions*)pImpl)->getWaitMode().ordinal() ; 
	return static_cast<SKWaitMode> (waitMode);
}

SKVersionConstraint * SKRetrievalOptions::getVersionConstraint() const {
	VersionConstraint *  pvc = new VersionConstraint( java_cast<VersionConstraint>( ((RetrievalOptions*)pImpl)->getVersionConstraint() )) ; 
	return new SKVersionConstraint(pvc);
}

SKNonExistenceResponse::SKNonExistenceResponse SKRetrievalOptions::getNonExistenceResponse() const {
	int  ner = (int)((RetrievalOptions*)pImpl)->getNonExistenceResponse().ordinal() ; 
	return static_cast<SKNonExistenceResponse::SKNonExistenceResponse> (ner);
}

bool SKRetrievalOptions::getVerifyChecksums() const {
	return  (bool)((RetrievalOptions*)pImpl)->getVerifyChecksums(); 
}

bool SKRetrievalOptions::getReturnInvalidations() const {
	return  (bool)((RetrievalOptions*)pImpl)->getReturnInvalidations(); 
}

SKForwardingMode SKRetrievalOptions::getForwardingMode() const
{
	int  fm = (int)((RetrievalOptions*)pImpl)->getForwardingMode().ordinal() ; 
	return static_cast<SKForwardingMode> (fm);
}

bool SKRetrievalOptions::getUpdateSecondariesOnMiss() const {
	return  (bool)((RetrievalOptions*)pImpl)->getUpdateSecondariesOnMiss(); 
}

////////

SKRetrievalOptions * SKRetrievalOptions::parse(const char * def)
{
	RetrievalOptions * pRetrievalOptions = new RetrievalOptions(java_cast<RetrievalOptions>(
			RetrievalOptions::parse(java_new<String>((char*)def))));
	return new SKRetrievalOptions(pRetrievalOptions);
}

bool SKRetrievalOptions::equals(SKRetrievalOptions * other) const {
	RetrievalOptions * pro = (RetrievalOptions *) other->pImpl;
	return  (bool)((RetrievalOptions*)pImpl)->equals(*pro); 
}

string SKRetrievalOptions::toString() const{
	string representation = (string)(((RetrievalOptions*)pImpl)->toString());
	return representation;
}

////////

SKRetrievalOptions::~SKRetrievalOptions()
{
	//FIXME: change for inheritance 
	if(pImpl!=NULL) {
		RetrievalOptions * po = (RetrievalOptions*)pImpl;
		delete po; 
		pImpl = NULL;
	}
}

SKRetrievalOptions::SKRetrievalOptions(SKOpTimeoutController * opTimeoutController,
            std::set<SKSecondaryTarget*> * secondaryTargets,
            SKRetrievalType retrievalType, 
            SKWaitMode waitMode, SKVersionConstraint * versionConstraint,
            SKNonExistenceResponse::SKNonExistenceResponse nonExistenceResponse, 
			bool verifyChecksums, bool returnInvalidations,
            SKForwardingMode forwardingMode, 
			bool updateSecondariesOnMiss)
{
	OpTimeoutController * pTimeoutCtrl = opTimeoutController->getPImpl();
	RetrievalType * pRt = ::getRetrievalType(retrievalType);
	WaitMode * pWm = ::getWaitMode(waitMode);
	NonExistenceResponse * pNer = getNonExistenceResponseType(nonExistenceResponse);
	VersionConstraint * pvc = (VersionConstraint *) versionConstraint->getPImpl();
	ForwardingMode * pFm = ::getForwardingMode(forwardingMode);

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

	pImpl = new RetrievalOptions(java_new<RetrievalOptions>( *pTimeoutCtrl, targets,
        *pRt, *pWm, *pvc, *pNer, JBoolean(verifyChecksums), JBoolean(returnInvalidations),
		*pFm, JBoolean(updateSecondariesOnMiss) )); 
	delete pNer;
	delete pRt;
	delete pWm;
	delete pFm;
}

SKRetrievalOptions::SKRetrievalOptions(void * pOpt) : pImpl(pOpt) {};  //FIXME: make protected ?
//protected:
SKRetrievalOptions::SKRetrievalOptions(){ pImpl = NULL; };

