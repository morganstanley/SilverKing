#include "StdAfx.h"
#include "MWaitOptions.h"
#include "MVersionConstraint.h"
#include "MWaitForTimeoutController.h"
#include "MSecondaryTarget.h"

#include <string>
using namespace std;
#include "skconstants.h"
#include "SKWaitOptions.h"
#include "SKVersionConstraint.h"
#include "SKWaitForTimeoutController.h"
#include "SKSecondaryTarget.h"

using namespace System::Runtime::InteropServices;

namespace SKManagedClient {


MWaitOptions ^ MWaitOptions::parse(String ^ def){
    SKWaitOptions * pwo = SKWaitOptions::parse((char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(def).ToPointer());
    SKWaitOptions_M ^ wo = gcnew SKWaitOptions_M;
    wo->pWaitOptions = pwo;
    return gcnew MWaitOptions(wo);
}

MWaitOptions::!MWaitOptions()
{
    if(pImpl)
    {
        delete (SKWaitOptions*)pImpl ;
        pImpl = NULL;
    }
}

MWaitOptions::~MWaitOptions()
{
    this->!MWaitOptions();
}

MWaitOptions::MWaitOptions(SKWaitOptions_M ^ opt)
{
    pImpl = opt->pWaitOptions;
}

SKWaitOptions_M ^ MWaitOptions::getPImpl() 
{
    SKWaitOptions_M ^ opt = gcnew SKWaitOptions_M;
    opt->pWaitOptions = pImpl;
    return opt;
}

MWaitOptions::MWaitOptions(SKRetrievalType_M retrievalType, MVersionConstraint ^ versionConstraint,
        SKNonExistenceResponse_M nonExistenceResponse, bool verifyChecksums,
        bool updateSecondariesOnMiss, int timeoutSeconds, int threshold, SKTimeoutResponse_M timeoutResponse)
{
        SKWaitOptions * pWaitOpt = new SKWaitOptions( 
            (SKRetrievalType) retrievalType,
            (SKVersionConstraint *)(versionConstraint->getPImpl()->pVersionConstraint),
            (SKNonExistenceResponse::SKNonExistenceResponse) nonExistenceResponse,
            verifyChecksums, updateSecondariesOnMiss, timeoutSeconds, threshold, 
            (SKTimeoutResponse::SKTimeoutResponse) timeoutResponse
        );
        pImpl = pWaitOpt;
}

MWaitOptions::MWaitOptions(SKRetrievalType_M retrievalType, MVersionConstraint ^ versionConstraint,
        SKNonExistenceResponse_M nonExistenceResponse, bool verifyChecksums, 
        bool updateSecondariesOnMiss, HashSet<MSecondaryTarget^> ^ secondaryTargets, int timeoutSeconds, int threshold, 
        SKTimeoutResponse_M timeoutResponse)
{
        std::set<SKSecondaryTarget*> * pTgtSet = new std::set<SKSecondaryTarget*>();
        System::Collections::Generic::IEnumerator<MSecondaryTarget^> ^ hse = secondaryTargets->GetEnumerator();
        while(hse->MoveNext()){
            MSecondaryTarget^ tgt = hse->Current;
            SKSecondaryTarget * pTgtg = (SKSecondaryTarget *) (tgt->getPImpl()->pSecondaryTarget);
            pTgtSet->insert(pTgtg);
        }

        SKWaitOptions * pWaitOpt = new SKWaitOptions( 
            (SKRetrievalType) retrievalType,
            (SKVersionConstraint *)(versionConstraint->getPImpl()->pVersionConstraint),
            (SKNonExistenceResponse::SKNonExistenceResponse) nonExistenceResponse,
            verifyChecksums, updateSecondariesOnMiss, 
            pTgtSet,
            timeoutSeconds, threshold, 
            (SKTimeoutResponse::SKTimeoutResponse) timeoutResponse
        );
        pImpl = pWaitOpt;
        delete pTgtSet;
}

MWaitOptions::MWaitOptions(MWaitForTimeoutController ^ opTimeoutController, SKRetrievalType_M retrievalType, 
        MVersionConstraint ^ versionConstraint, SKNonExistenceResponse_M nonExistenceResponse, 
        bool verifyChecksums, HashSet<MSecondaryTarget^> ^ secondaryTargets, int timeoutSeconds, int threshold, 
        SKTimeoutResponse_M timeoutResponse, bool updateSecondariesOnMiss )
{
        std::set<SKSecondaryTarget*> * pTgtSet = new std::set<SKSecondaryTarget*>();
        System::Collections::Generic::IEnumerator<MSecondaryTarget^> ^ hse = secondaryTargets->GetEnumerator();
        while(hse->MoveNext()){
            MSecondaryTarget^ tgt = hse->Current;
            SKSecondaryTarget * pTgtg = (SKSecondaryTarget *) (tgt->getPImpl()->pSecondaryTarget);
            pTgtSet->insert(pTgtg);
        }
        SKWaitForTimeoutController * pOpTimeoutCtrl = (SKWaitForTimeoutController *) (opTimeoutController->getPImpl()->pOpTimeoutController);
        SKWaitOptions * pWaitOpt = new SKWaitOptions( 
            pOpTimeoutCtrl,
            (SKRetrievalType) retrievalType,
            (SKVersionConstraint *)(versionConstraint->getPImpl()->pVersionConstraint),
            (SKNonExistenceResponse::SKNonExistenceResponse) nonExistenceResponse,
            verifyChecksums, pTgtSet, timeoutSeconds, threshold,
            (SKTimeoutResponse::SKTimeoutResponse) timeoutResponse,
            updateSecondariesOnMiss
        );
        pImpl = pWaitOpt;
        delete pTgtSet;
}

MWaitOptions::MWaitOptions(MWaitForTimeoutController ^ opTimeoutController, SKRetrievalType_M retrievalType, 
        MVersionConstraint ^ versionConstraint, SKNonExistenceResponse_M nonExistenceResponse, 
        bool verifyChecksums, int timeoutSeconds, int threshold, SKTimeoutResponse_M timeoutResponse, 
        bool updateSecondariesOnMiss )
{
        SKWaitForTimeoutController * pOpTimeoutCtrl = (SKWaitForTimeoutController *) (opTimeoutController->getPImpl()->pOpTimeoutController);
        SKWaitOptions * pWaitOpt = new SKWaitOptions( 
            pOpTimeoutCtrl,
            (SKRetrievalType) retrievalType,
            (SKVersionConstraint *)(versionConstraint->getPImpl()->pVersionConstraint),
            (SKNonExistenceResponse::SKNonExistenceResponse) nonExistenceResponse,
            verifyChecksums, timeoutSeconds, threshold,
            (SKTimeoutResponse::SKTimeoutResponse) timeoutResponse,
            updateSecondariesOnMiss
        );
        pImpl = pWaitOpt;
}

MWaitOptions::MWaitOptions(MWaitForTimeoutController ^ opTimeoutController, SKRetrievalType_M retrievalType, 
        MVersionConstraint ^ versionConstraint, int timeoutSeconds, int threshold, 
        SKTimeoutResponse_M timeoutResponse)
{
        SKWaitForTimeoutController * pOpTimeoutCtrl = (SKWaitForTimeoutController *) (opTimeoutController->getPImpl()->pOpTimeoutController);
        SKWaitOptions * pWaitOpt = new SKWaitOptions( 
            pOpTimeoutCtrl,
            (SKRetrievalType) retrievalType,
            (SKVersionConstraint *)(versionConstraint->getPImpl()->pVersionConstraint),
            timeoutSeconds, threshold,
            (SKTimeoutResponse::SKTimeoutResponse) timeoutResponse
        );
        pImpl = pWaitOpt;
}

MWaitOptions::MWaitOptions(MWaitForTimeoutController ^ opTimeoutController, SKRetrievalType_M retrievalType,
        MVersionConstraint ^ versionConstraint, int timeoutSeconds, int threshold)
{
        SKWaitForTimeoutController * pOpTimeoutCtrl = (SKWaitForTimeoutController *) (opTimeoutController->getPImpl()->pOpTimeoutController);
        SKWaitOptions * pWaitOpt = new SKWaitOptions( 
            pOpTimeoutCtrl,
            (SKRetrievalType) retrievalType,
            (SKVersionConstraint *)(versionConstraint->getPImpl()->pVersionConstraint),
            timeoutSeconds, threshold
        );
        pImpl = pWaitOpt;
}

MWaitOptions::MWaitOptions(SKRetrievalType_M retrievalType, MVersionConstraint ^ versionConstraint, int timeoutSeconds, int threshold, SKTimeoutResponse_M timeoutResponse)
{
        SKWaitOptions * pWaitOpt = new SKWaitOptions( 
            (SKRetrievalType) retrievalType,
            (SKVersionConstraint *)(versionConstraint->getPImpl()->pVersionConstraint),
            timeoutSeconds, threshold,
            (SKTimeoutResponse::SKTimeoutResponse) timeoutResponse
        );
        pImpl = pWaitOpt;
}

MWaitOptions::MWaitOptions(SKRetrievalType_M retrievalType, MVersionConstraint ^ versionConstraint, int timeoutSeconds, int threshold)
{
        SKWaitOptions * pWaitOpt = new SKWaitOptions( 
            (SKRetrievalType) retrievalType,
            (SKVersionConstraint *)(versionConstraint->getPImpl()->pVersionConstraint),
            timeoutSeconds, threshold
        );
        pImpl = pWaitOpt;
}

MWaitOptions::MWaitOptions(SKRetrievalType_M retrievalType, MVersionConstraint ^ versionConstraint, int timeoutSeconds)
{
        SKWaitOptions * pWaitOpt = new SKWaitOptions( 
            (SKRetrievalType) retrievalType,
            (SKVersionConstraint *)(versionConstraint->getPImpl()->pVersionConstraint),
            timeoutSeconds
        );
        pImpl = pWaitOpt;
}

MWaitOptions::MWaitOptions(SKRetrievalType_M retrievalType, MVersionConstraint ^ versionConstraint)
{
        SKWaitOptions * pWaitOpt = new SKWaitOptions( 
            (SKRetrievalType) retrievalType,
            (SKVersionConstraint *)(versionConstraint->getPImpl()->pVersionConstraint)
        );
        pImpl = pWaitOpt;
}

MWaitOptions::MWaitOptions(SKRetrievalType_M retrievalType)
{
        SKWaitOptions * pWaitOpt = new SKWaitOptions( (SKRetrievalType) retrievalType );
        pImpl = pWaitOpt;
}


MWaitOptions::MWaitOptions()
{
    pImpl = new SKWaitOptions();
}

MRetrievalOptions ^ MWaitOptions::retrievalType(SKRetrievalType_M retrievalType)
{
    ((SKWaitOptions*)pImpl)->retrievalType( (SKRetrievalType) retrievalType);
    return this;
}

MRetrievalOptions ^ MWaitOptions::versionConstraint(MVersionConstraint ^ versionConstraint)
{
    ((SKWaitOptions*)pImpl)->versionConstraint( (SKVersionConstraint*) (versionConstraint->getPImpl()->pVersionConstraint));
    return this;
}

MWaitOptions ^ MWaitOptions::timeoutSeconds(int timeoutSeconds)
{
    ((SKWaitOptions*)pImpl)->timeoutSeconds(timeoutSeconds);
    return this;
}

MWaitOptions ^ MWaitOptions::threshold(int threshold)
{
    ((SKWaitOptions*)pImpl)->threshold(threshold);
    return this;
}

MWaitOptions ^ MWaitOptions::timeoutResponse(SKTimeoutResponse_M timeoutResponse)
{
    ((SKWaitOptions*)pImpl)->timeoutResponse( (SKTimeoutResponse::SKTimeoutResponse) timeoutResponse);
    return this;
}

MRetrievalOptions ^ MWaitOptions::waitMode(SKWaitMode_M waitMode)
{
    ((SKWaitOptions*)pImpl)->waitMode((SKWaitMode)waitMode);
    return this;
}

MRetrievalOptions ^ MWaitOptions::nonExistenceResponse(SKNonExistenceResponse_M nonExistenceResponse)
{
    ((SKWaitOptions*)pImpl)->nonExistenceResponse((SKNonExistenceResponse::SKNonExistenceResponse)nonExistenceResponse);
    return this;
}


SKRetrievalType_M MWaitOptions::getRetrievalType() 
{
    SKRetrievalType rt = ((SKWaitOptions*)pImpl)->getRetrievalType();
    return (SKRetrievalType_M) rt;
}

SKWaitMode_M MWaitOptions::getWaitMode() 
{
    SKWaitMode wm = ((SKWaitOptions*)pImpl)->getWaitMode();
    return (SKWaitMode_M) wm;
}

MVersionConstraint ^ MWaitOptions::getVersionConstraint() 
{
    SKVersionConstraint * pvc = ((SKWaitOptions*)pImpl)->getVersionConstraint();
    SKVersionConstraint_M ^ vc_m = gcnew SKVersionConstraint_M;
    vc_m->pVersionConstraint  = pvc;
    MVersionConstraint ^ mvc = gcnew MVersionConstraint(vc_m);
    return mvc;
}

SKNonExistenceResponse_M MWaitOptions::getNonExistenceResponse() 
{
    SKNonExistenceResponse::SKNonExistenceResponse nr = ((SKWaitOptions*)pImpl)->getNonExistenceResponse();
    return (SKNonExistenceResponse_M) nr;
}

bool MWaitOptions::getVerifyChecksums() 
{
    return ((SKWaitOptions*)pImpl)->getVerifyChecksums();
}

int MWaitOptions::getTimeoutSeconds() 
{
    return ((SKWaitOptions*)pImpl)->getTimeoutSeconds();
}

int MWaitOptions::getThreshold() 
{
    return ((SKWaitOptions*)pImpl)->getThreshold();
}

SKTimeoutResponse_M MWaitOptions::getTimeoutResponse() 
{
    SKTimeoutResponse::SKTimeoutResponse rsp =  ((SKWaitOptions*)pImpl)->getTimeoutResponse();
    return (SKTimeoutResponse_M) rsp;
}

bool MWaitOptions::hasTimeout() 
{
    return ((SKWaitOptions*)pImpl)->hasTimeout();
}

String ^ MWaitOptions::toString() 
{
    string cppstr =  ((SKWaitOptions*)pImpl)->toString();
    System::String ^ str = gcnew System::String(cppstr.c_str());
    return str;
}

HashSet<MSecondaryTarget^> ^ MWaitOptions::getSecondaryTargets(){
    std::set<SKSecondaryTarget*> * pTgts = ((SKWaitOptions*)pImpl)->getSecondaryTargets();
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

bool MWaitOptions::equals(MRetrievalOptions ^ other){
    if( this->GetType() != MWaitOptions::typeid || other->GetType() != this->GetType())
        return false;
    return ((SKWaitOptions*)pImpl)->equals((SKWaitOptions*)(((MWaitOptions^)other)->getPImpl()->pWaitOptions));
}

MRetrievalOptions ^ MWaitOptions::updateSecondariesOnMiss(bool updateSecondariesOnMiss)
{
    ((SKWaitOptions*)pImpl)->updateSecondariesOnMiss( updateSecondariesOnMiss );
    return this;
}

MRetrievalOptions ^ MWaitOptions::secondaryTargets(HashSet<MSecondaryTarget^> ^ secondaryTargets){
    std::set<SKSecondaryTarget*> * pTgtSet = new std::set<SKSecondaryTarget*>();
    System::Collections::Generic::IEnumerator<MSecondaryTarget^> ^ hse = secondaryTargets->GetEnumerator();
    while(hse->MoveNext()){
        MSecondaryTarget^ tgt = hse->Current;
        SKSecondaryTarget * pTgtg = (SKSecondaryTarget *) (tgt->getPImpl()->pSecondaryTarget);
        pTgtSet->insert(pTgtg);
    }
    ((SKWaitOptions*)pImpl)->secondaryTargets( pTgtSet );
    delete pTgtSet;
    return this;
}

bool MWaitOptions::getUpdateSecondariesOnMiss() 
{
    return ((SKWaitOptions*)pImpl)->getUpdateSecondariesOnMiss();
}


}