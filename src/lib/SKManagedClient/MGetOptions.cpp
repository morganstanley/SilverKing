#include "StdAfx.h"
#include "MGetOptions.h"
#include "MVersionConstraint.h"
#include "MOpTimeoutController.h"
#include "MSecondaryTarget.h"

#include <string>
using namespace std;
#include "skconstants.h"
#include "SKGetOptions.h"
#include "SKVersionConstraint.h"
#include "SKOpTimeoutController.h"
#include "SKSecondaryTarget.h"


namespace SKManagedClient {

MGetOptions::!MGetOptions()
{
    if(pImpl) 
    {
        delete (SKGetOptions*)pImpl ;
        pImpl = NULL;
    }
}

MGetOptions::~MGetOptions()
{
    this->!MGetOptions();
}

MGetOptions::MGetOptions(SKRetrievalType_M retrievalType, MVersionConstraint ^ versionConstraint)
{
    SKGetOptions * pGetOpt = new SKGetOptions( 
        (SKRetrievalType) retrievalType,
        (SKVersionConstraint *)(versionConstraint->getPImpl()->pVersionConstraint)
    );
    pImpl = pGetOpt;
}

MGetOptions::MGetOptions(SKRetrievalType_M retrievalType)
{
    SKGetOptions * pGetOpt = new SKGetOptions( (SKRetrievalType) retrievalType );
    pImpl = pGetOpt;
}

MGetOptions::MGetOptions(MOpTimeoutController ^ opTimeoutController, SKRetrievalType_M retrievalType, 
            MVersionConstraint ^ versionConstraint, SKNonExistenceResponse_M nonExistenceResponse, 
            bool verifyChecksums,  bool updateSecondariesOnMiss, HashSet<MSecondaryTarget^> ^ secondaryTargets)
{
    SKOpTimeoutController * pOpTimeoutCtrl = (SKOpTimeoutController *) (opTimeoutController->getPImpl()->pOpTimeoutController);
    std::set<SKSecondaryTarget*> * pTgtSet = new std::set<SKSecondaryTarget*>();
    System::Collections::Generic::IEnumerator<MSecondaryTarget^> ^ hse = secondaryTargets->GetEnumerator();
    while(hse->MoveNext()){
        MSecondaryTarget^ tgt = hse->Current;
        SKSecondaryTarget * pTgtg = (SKSecondaryTarget *) (tgt->getPImpl()->pSecondaryTarget);
        pTgtSet->insert(pTgtg);
    }

    SKGetOptions * pGetOpt = new SKGetOptions( 
        pOpTimeoutCtrl, 
        (SKRetrievalType) retrievalType,
        (SKVersionConstraint *)(versionConstraint->getPImpl()->pVersionConstraint),
        (SKNonExistenceResponse::SKNonExistenceResponse) nonExistenceResponse,
        verifyChecksums, 
        updateSecondariesOnMiss, 
        pTgtSet
    );
    delete pTgtSet;
    pImpl = pGetOpt;
}

MGetOptions::MGetOptions(MOpTimeoutController ^ opTimeoutController, SKRetrievalType_M retrievalType, 
            MVersionConstraint ^ versionConstraint)
{
    SKOpTimeoutController * pOpTimeoutCtrl = (SKOpTimeoutController *) (opTimeoutController->getPImpl()->pOpTimeoutController);
    SKGetOptions * pGetOpt = new SKGetOptions( 
        pOpTimeoutCtrl,
        (SKRetrievalType) retrievalType,
        (SKVersionConstraint *)(versionConstraint->getPImpl()->pVersionConstraint)
    );
    pImpl = pGetOpt;
}

MGetOptions::MGetOptions(SKGetOptions_M ^ opt)
{
    pImpl = opt->pGetOptions;
}

SKGetOptions_M ^ MGetOptions::getPImpl()  
{
    SKGetOptions_M ^ opt = gcnew SKGetOptions_M;
    opt->pGetOptions = pImpl;
    return opt;
}

//methods
MRetrievalOptions ^ MGetOptions::retrievalType(SKRetrievalType_M retrievalType) 
{
    ((SKGetOptions*)pImpl)->retrievalType((SKRetrievalType)retrievalType);
    return this;
}

MRetrievalOptions ^ MGetOptions::versionConstraint(MVersionConstraint ^ versionConstraint)
{
    ((SKGetOptions*)pImpl)->versionConstraint( (SKVersionConstraint*) (versionConstraint->getPImpl()->pVersionConstraint));
    return this;
}

MRetrievalOptions ^ MGetOptions::waitMode(SKWaitMode_M waitMode) 
{
    ((SKGetOptions*)pImpl)->waitMode((SKWaitMode)waitMode);
    return this;
}

MRetrievalOptions ^ MGetOptions::nonExistenceResponse(SKNonExistenceResponse_M nonExistenceResponse)
{
    ((SKGetOptions*)pImpl)->nonExistenceResponse((SKNonExistenceResponse::SKNonExistenceResponse)nonExistenceResponse);
    return this;
}

SKRetrievalType_M MGetOptions::getRetrievalType()
{
    SKRetrievalType rt = ((SKGetOptions*)pImpl)->getRetrievalType();
    return (SKRetrievalType_M) rt;
}

SKWaitMode_M MGetOptions::getWaitMode()
{
    SKWaitMode wm = ((SKGetOptions*)pImpl)->getWaitMode();
    return (SKWaitMode_M) wm;

}

MVersionConstraint ^ MGetOptions::getVersionConstraint()
{
    SKVersionConstraint * pvc = ((SKGetOptions*)pImpl)->getVersionConstraint();
    SKVersionConstraint_M ^ vc_m = gcnew SKVersionConstraint_M;
    vc_m->pVersionConstraint  = pvc;
    MVersionConstraint ^ mvc = gcnew MVersionConstraint(vc_m);
    return mvc;
}

SKNonExistenceResponse_M MGetOptions::getNonExistenceResponse()
{
    SKNonExistenceResponse::SKNonExistenceResponse nr = ((SKGetOptions*)pImpl)->getNonExistenceResponse();
    return (SKNonExistenceResponse_M) nr;
}

bool MGetOptions::getVerifyChecksums()
{
    return ((SKGetOptions*)pImpl)->getVerifyChecksums();
}

String ^ MGetOptions::toString()
{
    string cppstr =  ((SKGetOptions*)pImpl)->toString();
    System::String ^ str = gcnew System::String(cppstr.c_str());
    return str;
}

/*
bool MGetOptions::equals(MGetOptions ^ other) 
{
    SKGetOptions* pgo = (SKGetOptions*)(other->getPImpl()->pGetOptions)
    return ((SKGetOptions*)pImpl)->equals(pgo);
}

bool operator == (MGetOptions ^ go1,  MGetOptions ^ go2) const 
{
    return go1->equals(go2); //FIXME equals should be expressed through operator ==
}
*/

MRetrievalOptions ^ MGetOptions::updateSecondariesOnMiss(bool updateSecondariesOnMiss){
    ((SKGetOptions*)pImpl)->updateSecondariesOnMiss(updateSecondariesOnMiss);
    return this;
}

bool MGetOptions::getUpdateSecondariesOnMiss(){
    return ((SKGetOptions*)pImpl)->getUpdateSecondariesOnMiss();
}

MRetrievalOptions ^ MGetOptions::secondaryTargets(HashSet<MSecondaryTarget^> ^ secondaryTargets){
    std::set<SKSecondaryTarget*> * pTgtSet = new std::set<SKSecondaryTarget*>();
    System::Collections::Generic::IEnumerator<MSecondaryTarget^> ^ hse = secondaryTargets->GetEnumerator();
    while(hse->MoveNext()){
        MSecondaryTarget^ tgt = hse->Current;
        SKSecondaryTarget * pTgtg = (SKSecondaryTarget *) (tgt->getPImpl()->pSecondaryTarget);
        pTgtSet->insert(pTgtg);
    }
    ((SKGetOptions*)pImpl)->secondaryTargets( pTgtSet );
    delete pTgtSet;
    return this;
}

HashSet<MSecondaryTarget^> ^ MGetOptions::getSecondaryTargets(){
    std::set<SKSecondaryTarget*> * pTgts = ((SKGetOptions*)pImpl)->getSecondaryTargets();
    std::set<SKSecondaryTarget*>::iterator it;
    HashSet< MSecondaryTarget^ > ^ secondaryTargets = gcnew HashSet< MSecondaryTarget^ >();
    for(it=pTgts->begin(); it!=pTgts->end(); it++)
    {
        SKSecondaryTarget * pTarget = *it;
        SKSecondaryTarget_M ^ target = gcnew SKSecondaryTarget_M;
        target->pSecondaryTarget = pTarget;
        MSecondaryTarget ^ secondaryTarget = gcnew MSecondaryTarget(target);
        secondaryTargets->Add(secondaryTarget);
    }
    delete pTgts; pTgts = NULL;

    return secondaryTargets;
}

bool MGetOptions::equals(MRetrievalOptions ^ other){
    if( this->GetType() != MGetOptions::typeid || other->GetType() != this->GetType())
        return false;
    return ((SKGetOptions*)pImpl)->equals((SKGetOptions*)(((MGetOptions^)other)->getPImpl()->pGetOptions));
}


}