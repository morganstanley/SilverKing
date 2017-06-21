#include <exception>
#include "jenumutil.h"
#include "SKWaitOptions.h"
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
#include "jace/proxy/com/ms/silverking/cloud/dht/RetrievalType.h"
using jace::proxy::com::ms::silverking::cloud::dht::RetrievalType;
#include "jace/proxy/com/ms/silverking/cloud/dht/TimeoutResponse.h"
using jace::proxy::com::ms::silverking::cloud::dht::TimeoutResponse;
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
#include "jace/proxy/com/ms/silverking/cloud/dht/SecondaryTarget.h"
using jace::proxy::com::ms::silverking::cloud::dht::SecondaryTarget;

SKWaitOptions * SKWaitOptions::opTimeoutController(SKOpTimeoutController * opTimeoutController) {
    //OpTimeoutController * pOpTimeoutController = ::getOpTimeoutController(opTimeoutController);
	//OpTimeoutController *pOpTimeoutController = java_cast<OpTimeoutController>( *(opTimeoutController->getPImpl()) );
    OpTimeoutController *pOpTimeoutController = NULL; // FIXME
	WaitOptions * p = new WaitOptions(java_cast<WaitOptions>(
		((WaitOptions*)pImpl)->opTimeoutController(*pOpTimeoutController)
	)); 
    delete ((WaitOptions*)pImpl);
    pImpl = p;
    delete pOpTimeoutController;
    return this;
}

SKWaitOptions * SKWaitOptions::secondaryTargets(std::set<SKSecondaryTarget*> * secondaryTargets)
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

	WaitOptions * pRetrOptImp = new WaitOptions(java_cast<WaitOptions>(
		((WaitOptions*)pImpl)->secondaryTargets(targets)
	)); 
    delete ((WaitOptions*)pImpl);
    pImpl = pRetrOptImp;
    return this;
}

SKWaitOptions * SKWaitOptions::retrievalType(SKRetrievalType retrievalType){
	RetrievalType * pRetrievalType = ::getRetrievalType(retrievalType);
	WaitOptions * pWaitOptImp = new WaitOptions(java_cast<WaitOptions>(
		((WaitOptions*)pImpl)->retrievalType(*pRetrievalType)
	)); 
	delete pRetrievalType;
    delete ((WaitOptions*)pImpl);
    pImpl = pWaitOptImp;
    return this;
} 

SKWaitOptions * SKWaitOptions::versionConstraint(SKVersionConstraint * versionConstraint){
	VersionConstraint * pvc = (VersionConstraint *) versionConstraint->getPImpl();  //FIXME: friend
	WaitOptions * pWaitOptImp = new WaitOptions(java_cast<WaitOptions>(
		((WaitOptions*)pImpl)->versionConstraint(*pvc)
	)); 
    delete ((WaitOptions*)pImpl);
    pImpl = pWaitOptImp;
    return this;
}

SKWaitOptions * SKWaitOptions::nonExistenceResponse(SKNonExistenceResponse::SKNonExistenceResponse nonExistenceResponse){
	NonExistenceResponse * pNer = ::getNonExistenceResponseType(nonExistenceResponse);
	WaitOptions * p = new WaitOptions(java_cast<WaitOptions>(
		((WaitOptions*)pImpl)->nonExistenceResponse(*pNer)
	)); 
	delete pNer;
    delete ((WaitOptions*)pImpl);
    pImpl = p;
    return this;
}

SKWaitOptions * SKWaitOptions::verifyChecksums(bool verifyChecksums)
{
	WaitOptions * p = new WaitOptions(java_cast<WaitOptions>(
		((WaitOptions*)pImpl)->verifyChecksums(JBoolean(verifyChecksums))
	)); 
    delete ((WaitOptions*)pImpl);
    pImpl = p;
    return this;
}

SKWaitOptions * SKWaitOptions::returnInvalidations(bool returnInvalidations)
{
	WaitOptions * p = new WaitOptions(java_cast<WaitOptions>(
		((WaitOptions*)pImpl)->returnInvalidations(JBoolean(returnInvalidations))
	)); 
    delete ((WaitOptions*)pImpl);
    pImpl = p;
    return this;
}

SKWaitOptions * SKWaitOptions::updateSecondariesOnMiss(bool updateSecondariesOnMiss)
{
	WaitOptions * p = new WaitOptions(java_cast<WaitOptions>(
		((WaitOptions*)pImpl)->updateSecondariesOnMiss(JBoolean(updateSecondariesOnMiss))
	)); 
    delete ((WaitOptions*)pImpl);
    pImpl = p;
    return this;
}

SKWaitOptions * SKWaitOptions::timeoutSeconds(int timeoutSeconds){
	WaitOptions * p = new WaitOptions(java_cast<WaitOptions>(
		((WaitOptions*)pImpl)->timeoutSeconds(timeoutSeconds)
	)); 
    delete ((WaitOptions*)pImpl);
    pImpl = p;
    return this;
}

SKWaitOptions * SKWaitOptions::threshold(int threshold){
	WaitOptions * pWaitOptImp = new WaitOptions(java_cast<WaitOptions>(
		((WaitOptions*)pImpl)->threshold(threshold)
	)); 
    delete ((WaitOptions*)pImpl);
    pImpl = pWaitOptImp;
    return this;
}

SKWaitOptions * SKWaitOptions::timeoutResponse(SKTimeoutResponse::SKTimeoutResponse timeoutResponse){
	TimeoutResponse * pTr = ::getTimeoutResponse(timeoutResponse);
	WaitOptions * pWaitOptImp = new WaitOptions(java_cast<WaitOptions>(
		((WaitOptions*)pImpl)->timeoutResponse(*pTr)
	)); 
	delete pTr;
    delete ((WaitOptions*)pImpl);
    pImpl = pWaitOptImp;
    return this;
}

////////

int SKWaitOptions::getTimeoutSeconds(){
	int ts = (int)((WaitOptions*)pImpl)->getTimeoutSeconds() ; 
	return ts;
}

int SKWaitOptions::getThreshold(){
	int threshold = (int)((WaitOptions*)pImpl)->getThreshold() ; 
	return threshold;
}

SKTimeoutResponse::SKTimeoutResponse SKWaitOptions::getTimeoutResponse(){
	int  tr = (int)((WaitOptions*)pImpl)->getTimeoutResponse().ordinal() ; 
	return static_cast<SKTimeoutResponse::SKTimeoutResponse> (tr);
}

bool SKWaitOptions::hasTimeout(){
	return  (bool)((WaitOptions*)pImpl)->hasTimeout(); 
}

////////

SKWaitOptions * SKWaitOptions::parse(const char * def)
{
	WaitOptions * pWaitOptions = new WaitOptions(java_cast<WaitOptions>(
			WaitOptions::parse(java_new<String>((char*)def))));
	return new SKWaitOptions(pWaitOptions);
}

bool SKWaitOptions::equals(SKWaitOptions * other) const {  //FIXME: do SK need this on c/cpp side?
	WaitOptions * pro = (WaitOptions *) other->pImpl;
	return  (bool)((WaitOptions*)pImpl)->equals(*pro); 
}

string SKWaitOptions::toString(){
	string representation = (string)(((WaitOptions*)pImpl)->toString());
	return representation;
}

////////

SKWaitOptions::SKWaitOptions(void * pOpt) : SKRetrievalOptions(pOpt) {};  //FIXME: make protected ?
void * SKWaitOptions::getPImpl() {return pImpl;}  //FIXME:

SKWaitOptions::SKWaitOptions(SKOpTimeoutController * opTimeoutController, 
            std::set<SKSecondaryTarget*> * secondaryTargets,
            SKRetrievalType retrievalType, 
			SKVersionConstraint * versionConstraint, SKNonExistenceResponse::SKNonExistenceResponse nonExistenceResponse, 
			bool verifyChecksums, bool returnInvalidations, bool updateSecondariesOnMiss,
            int timeoutSeconds, int threshold, 
			SKTimeoutResponse::SKTimeoutResponse timeoutResponse)
{
	OpTimeoutController * pTimeoutCtrl = dynamic_cast<OpTimeoutController*>(opTimeoutController->getPImpl());
	RetrievalType * pRt = ::getRetrievalType(retrievalType);
    TimeoutResponse * pTimeoutResp = ::getTimeoutResponse(timeoutResponse);
	NonExistenceResponse * pNer = ::getNonExistenceResponseType(nonExistenceResponse);
	VersionConstraint * pvc = (VersionConstraint *) versionConstraint->getPImpl(); 

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

	//pImpl = new WaitOptions(java_new<WaitOptions>(*pTimeoutCtrl, targets, *pRt, *pvc, *pNer, 
    //    JBoolean(verifyChecksums), JBoolean(returnInvalidations), 
    //    JBoolean(updateSecondariesOnMiss),
	//	timeoutSeconds, threshold, *pTimeoutResp)); 
	pImpl = WaitOptions::Factory::create(*pTimeoutCtrl, targets, *pRt, *pvc, *pNer, 
        JBoolean(verifyChecksums), JBoolean(returnInvalidations), 
        JBoolean(updateSecondariesOnMiss),
		timeoutSeconds, threshold, *pTimeoutResp); 
    delete pTimeoutResp;
	delete pRt;
	delete pNer;
}

SKWaitOptions::~SKWaitOptions()
{
	//FIXME:  
	if(pImpl!=NULL) {
		WaitOptions * wo = (WaitOptions*)pImpl;
		delete wo; 
		pImpl = NULL;
	}
}

