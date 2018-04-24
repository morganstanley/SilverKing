#include "jenumutil.h"
#include "SKGetOptions.h"
#include "SKVersionConstraint.h"
#include "SKOpTimeoutController.h"
#include "SKSecondaryTarget.h"
#include "SKClientException.h"
#include <string>
using std::string;
#include <exception>
using std::exception;
#include <iostream>
using std::cout;
using std::endl;
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
#include "jace/proxy/com/ms/silverking/cloud/dht/GetOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::GetOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/RetrievalType.h"
using jace::proxy::com::ms::silverking::cloud::dht::RetrievalType;
#include "jace/proxy/com/ms/silverking/cloud/dht/VersionConstraint.h"
using jace::proxy::com::ms::silverking::cloud::dht::VersionConstraint;
#include "jace/proxy/com/ms/silverking/cloud/dht/VersionConstraint_Mode.h"
using jace::proxy::com::ms::silverking::cloud::dht::VersionConstraint_Mode;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/OpTimeoutController.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::OpTimeoutController;
#include "jace/proxy/com/ms/silverking/cloud/dht/NonExistenceResponse.h"
using jace::proxy::com::ms::silverking::cloud::dht::NonExistenceResponse;
#include "jace/proxy/com/ms/silverking/cloud/dht/SecondaryTarget.h"
using jace::proxy::com::ms::silverking::cloud::dht::SecondaryTarget;


SKGetOptions * SKGetOptions::opTimeoutController(SKOpTimeoutController * opTimeoutController) {
/*
    //OpTimeoutController * pOpTimeoutController = ::getOpTimeoutController(opTimeoutController);
    // FIXME - probably remove op timeout controller from C++ interface
	//OpTimeoutController *pOpTimeoutController = java_cast<OpTimeoutController>( *(opTimeoutController->getPImpl()) );
    opTimeoutController = NULL;
	GetOptions * pGoImp = new GetOptions(java_cast<GetOptions>(
		((GetOptions*)pImpl)->opTimeoutController(*pOpTimeoutController)
	)); 
    delete ((GetOptions*)pImpl);
    pImpl = pGoImp;
    delete pOpTimeoutController;
    return this;
    */
    return NULL;
}

SKGetOptions * SKGetOptions::secondaryTargets(std::set<SKSecondaryTarget*> * secondaryTargets)
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

	GetOptions * pRetrOptImp = new GetOptions(java_cast<GetOptions>(
		((GetOptions*)pImpl)->secondaryTargets(targets)
	)); 
    delete ((GetOptions*)pImpl);
    pImpl = pRetrOptImp;
    return this;
}

SKGetOptions * SKGetOptions::retrievalType(SKRetrievalType retrievalType) {
    RetrievalType * pRt = ::getRetrievalType(retrievalType);
	GetOptions * pGoImp = new GetOptions(java_cast<GetOptions>(
		((GetOptions*)pImpl)->retrievalType(*pRt)
	)); 
    return new SKGetOptions(pGoImp);
    /*
    delete ((GetOptions*)pImpl);
    pImpl = pGoImp;
    delete pRt;
    return this;
    */
}

SKGetOptions * SKGetOptions::versionConstraint(SKVersionConstraint * versionConstraint) {
	VersionConstraint * pVc = (VersionConstraint *) versionConstraint->getPImpl();
	GetOptions * pGoImp = new GetOptions(java_cast<GetOptions>(
		((GetOptions*)pImpl)->versionConstraint(*pVc)
	)); 
    return new SKGetOptions(pGoImp);
    /*
    delete ((GetOptions*)pImpl);
    pImpl = pGoImp;
    delete pVc;
    return this;
    */
}

SKGetOptions * SKGetOptions::nonExistenceResponse(SKNonExistenceResponse::SKNonExistenceResponse nonExistenceResponse){
	NonExistenceResponse * pNer = ::getNonExistenceResponseType(nonExistenceResponse);
	GetOptions * p = new GetOptions(java_cast<GetOptions>(
		((GetOptions*)pImpl)->nonExistenceResponse(*pNer)
	)); 
    return new SKGetOptions(p);
    /*
	delete pNer;
    delete ((GetOptions*)pImpl);
    pImpl = p;
    return this;
    */
}

SKGetOptions * SKGetOptions::verifyChecksums(bool verifyChecksums)
{
	GetOptions * p = new GetOptions(java_cast<GetOptions>(
		((GetOptions*)pImpl)->verifyChecksums(JBoolean(verifyChecksums))
	)); 
    delete ((GetOptions*)pImpl);
    pImpl = p;
    return this;
}

SKGetOptions * SKGetOptions::returnInvalidations(bool returnInvalidations)
{
	GetOptions * p = new GetOptions(java_cast<GetOptions>(
		((GetOptions*)pImpl)->returnInvalidations(JBoolean(returnInvalidations))
	)); 
    delete ((GetOptions*)pImpl);
    pImpl = p;
    return this;
}

SKGetOptions * SKGetOptions::updateSecondariesOnMiss(bool updateSecondariesOnMiss)
{
	GetOptions * p = new GetOptions(java_cast<GetOptions>(
		((GetOptions*)pImpl)->updateSecondariesOnMiss(JBoolean(updateSecondariesOnMiss))
	)); 
    delete ((GetOptions*)pImpl);
    pImpl = p;
    return this;
}

SKGetOptions * SKGetOptions::forwardingMode(SKForwardingMode forwardingMode)
{
	ForwardingMode * pFm = ::getForwardingMode(forwardingMode);
	GetOptions * pGetOptImp = new GetOptions(java_cast<GetOptions>(
		((GetOptions*)pImpl)->forwardingMode(*pFm)
	)); 
	delete pFm;
    delete ((GetOptions*)pImpl);
    pImpl = pGetOptImp;
    return this;
}

SKForwardingMode SKGetOptions::getForwardingMode() const
{
	int  fm = (int)((GetOptions*)pImpl)->getForwardingMode().ordinal() ; 
	return static_cast<SKForwardingMode> (fm);
}

////////

SKGetOptions * SKGetOptions::parse(const char * def)
{
	GetOptions * pGetOptions = new GetOptions(java_cast<GetOptions>(
			GetOptions::parse(java_new<String>((char*)def))));
	return new SKGetOptions(pGetOptions);
}

string SKGetOptions::toString()
{
	string representation = (string)(((GetOptions*)pImpl)->toString());
	return representation;
}

bool SKGetOptions::equals(SKGetOptions * other) const {
	GetOptions * pro = (GetOptions *) other->pImpl;
	return  (bool)((GetOptions*)pImpl)->equals(*pro); 
}

////////

SKGetOptions::SKGetOptions(SKOpTimeoutController * opTimeoutController, 
                        std::set<SKSecondaryTarget*> * secondaryTargets,
                        SKRetrievalType retrievalType, 
                        SKVersionConstraint * versionConstraint, 
                        SKNonExistenceResponse::SKNonExistenceResponse nonExistenceResponse, 
                        bool verifyChecksums, bool returnInvalidations,
                        SKForwardingMode forwardingMode,
                        bool updateSecondariesOnMiss)
{
	OpTimeoutController * pTimeoutCtrl = opTimeoutController->getPImpl();
	VersionConstraint * pvc = (VersionConstraint *) versionConstraint->getPImpl();  //FIXME: friend
	RetrievalType * pRt = ::getRetrievalType(retrievalType);
	NonExistenceResponse * pNer = ::getNonExistenceResponseType(nonExistenceResponse);
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

	pImpl = new GetOptions(java_new<GetOptions>(*pTimeoutCtrl, targets,
        *pRt, *pvc, *pNer, JBoolean(verifyChecksums), JBoolean(returnInvalidations),
        *pFm, 
		JBoolean(updateSecondariesOnMiss) )); 
	delete pRt;
	delete pNer;
	delete pFm;
}

SKGetOptions::SKGetOptions(void * pOpt) : SKRetrievalOptions(pOpt) {};  //FIXME: make protected ?
void * SKGetOptions::getPImpl() const {return pImpl;}  //FIXME:


SKGetOptions::~SKGetOptions()
{
	//FIXME: change for inheritance 
	if(pImpl!=NULL) {
		GetOptions * go = (GetOptions*)pImpl;
		delete go; 
		pImpl = NULL;
	}
}

