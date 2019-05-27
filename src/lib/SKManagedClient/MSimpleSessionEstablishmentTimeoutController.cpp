#include "StdAfx.h"
#include "MSimpleSessionEstablishmentTimeoutController.h"
#include "MSessionOptions.h"

#include <cstddef>
#include "SKSimpleSessionEstablishmentTimeoutController.h"
#include "SKSessionOptions.h"

using namespace System;
using namespace System::Net;
using namespace System::Runtime::InteropServices;

namespace SKManagedClient {

MSimpleSessionEstablishmentTimeoutController ^ MSimpleSessionEstablishmentTimeoutController::parse(String ^ def){
    SKSimpleSessionEstablishmentTimeoutController * ptc = SKSimpleSessionEstablishmentTimeoutController::parse((char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(def).ToPointer());
    SKSessionEstablishmentTimeoutController_M ^ tc = gcnew SKSessionEstablishmentTimeoutController_M;
    tc->pSessionEstablishmentTimeoutController = ptc;
    return gcnew MSimpleSessionEstablishmentTimeoutController(tc);

}

MSimpleSessionEstablishmentTimeoutController::~MSimpleSessionEstablishmentTimeoutController()
{
    this->!MSimpleSessionEstablishmentTimeoutController();
}

MSimpleSessionEstablishmentTimeoutController::!MSimpleSessionEstablishmentTimeoutController()
{
    if(pImpl)
    {
        delete (SKSimpleSessionEstablishmentTimeoutController*)pImpl ;
        pImpl = NULL;
    }
}

//MSimpleSessionEstablishmentTimeoutController::MSimpleSessionEstablishmentTimeoutController() : pImpl(NULL) { ; };

MSimpleSessionEstablishmentTimeoutController::MSimpleSessionEstablishmentTimeoutController(int maxAttempts, int attemptRelativeTimeoutMillis, int maxRelativeTimeoutMillis) 
    : MSessionEstablishmentTimeoutController()
{
    SKSimpleSessionEstablishmentTimeoutController * pSessCtrl = new SKSimpleSessionEstablishmentTimeoutController(maxAttempts, attemptRelativeTimeoutMillis, maxRelativeTimeoutMillis);
    pImpl = pSessCtrl;
}

//internal method
MSimpleSessionEstablishmentTimeoutController::MSimpleSessionEstablishmentTimeoutController(SKSessionEstablishmentTimeoutController_M ^ sessController) 
    : MSessionEstablishmentTimeoutController(sessController) { ; }

SKSessionEstablishmentTimeoutController_M ^ MSimpleSessionEstablishmentTimeoutController::getPImpl(){
    SKSessionEstablishmentTimeoutController_M ^ opc = gcnew SKSessionEstablishmentTimeoutController_M;
    opc->pSessionEstablishmentTimeoutController = pImpl;
    return opc;
}

// public methods
int MSimpleSessionEstablishmentTimeoutController::getMaxAttempts(MSessionOptions ^ sessOpt){
    SKSessionOptions * pSessOpt = (SKSessionOptions *) (sessOpt->getPImpl()->pSessOptions);
    return ((SKSimpleSessionEstablishmentTimeoutController*)pImpl)->getMaxAttempts(pSessOpt);
}

int MSimpleSessionEstablishmentTimeoutController::getRelativeTimeoutMillisForAttempt(MSessionOptions ^ sessOpt, int attemptIndex){
    SKSessionOptions * pSessOpt = (SKSessionOptions *) (sessOpt->getPImpl()->pSessOptions);
    return ((SKSimpleSessionEstablishmentTimeoutController*)pImpl)->getRelativeTimeoutMillisForAttempt(pSessOpt, attemptIndex);
}

int MSimpleSessionEstablishmentTimeoutController::getMaxRelativeTimeoutMillis(MSessionOptions ^ sessOpt){
    SKSessionOptions * pSessOpt = (SKSessionOptions *) (sessOpt->getPImpl()->pSessOptions);
    return ((SKSimpleSessionEstablishmentTimeoutController*)pImpl)->getMaxRelativeTimeoutMillis(pSessOpt);
}

String ^ MSimpleSessionEstablishmentTimeoutController::toString(){
    std::string stdstr =  ((SKSimpleSessionEstablishmentTimeoutController*)pImpl)->toString();
    String ^ str = gcnew String( stdstr.c_str(), 0, stdstr.size() );
    return str;
}

MSimpleSessionEstablishmentTimeoutController ^ MSimpleSessionEstablishmentTimeoutController::maxAttempts(int maxAttempts){
    ((SKSimpleSessionEstablishmentTimeoutController*)pImpl)->maxAttempts(maxAttempts);
    return this;
}
MSimpleSessionEstablishmentTimeoutController ^ MSimpleSessionEstablishmentTimeoutController::maxRelativeTimeoutMillis(int maxRelativeTimeoutMillis)
{
    ((SKSimpleSessionEstablishmentTimeoutController*)pImpl)->maxRelativeTimeoutMillis(maxRelativeTimeoutMillis);
    return this;
}

MSimpleSessionEstablishmentTimeoutController ^ MSimpleSessionEstablishmentTimeoutController::attemptRelativeTimeoutMillis(int attemptRelativeTimeoutMillis) 
{
    ((SKSimpleSessionEstablishmentTimeoutController*)pImpl)->attemptRelativeTimeoutMillis(attemptRelativeTimeoutMillis);
    return this;
}


}