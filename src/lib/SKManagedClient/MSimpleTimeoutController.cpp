#include "StdAfx.h"
#include "MSimpleTimeoutController.h"
#include "SKAsyncOperation.h"

#include <cstddef>
#include "SKSimpleTimeoutController.h"
#include "MAsyncOperation.h"

using namespace System;
using namespace System::Net;
using namespace System::Runtime::InteropServices;

namespace SKManagedClient {

MSimpleTimeoutController ^ MSimpleTimeoutController::parse(String ^ def){
    SKSimpleTimeoutController * ptc = SKSimpleTimeoutController::parse((char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(def).ToPointer());
    SKOpTimeoutController_M ^ tc = gcnew SKOpTimeoutController_M;
    tc->pOpTimeoutController = ptc;
    return gcnew MSimpleTimeoutController(tc);

}

MSimpleTimeoutController::~MSimpleTimeoutController()
{
    this->!MSimpleTimeoutController();
}

MSimpleTimeoutController::!MSimpleTimeoutController()
{
    if(pImpl)
    {
        delete (SKSimpleTimeoutController*)pImpl ;
        pImpl = NULL;
    }
}

MSimpleTimeoutController::MSimpleTimeoutController(int maxAttempts, int maxRelativeTimeoutMillis) {
    SKSimpleTimeoutController * pOpCtrl = new SKSimpleTimeoutController(maxAttempts, maxRelativeTimeoutMillis);
    pImpl = pOpCtrl;
}

//internal methods
MSimpleTimeoutController::MSimpleTimeoutController(SKOpTimeoutController_M ^ opController) {
    pImpl = opController->pOpTimeoutController; //(SKSimpleTimeoutController *)
}

SKOpTimeoutController_M ^ MSimpleTimeoutController::getPImpl(){
    SKOpTimeoutController_M ^ opc = gcnew SKOpTimeoutController_M;
    opc->pOpTimeoutController = pImpl;
    return opc;
}

// public methods
int MSimpleTimeoutController::getMaxAttempts(MAsyncOperation ^ op){
    SKAsyncOperation * pOp = (SKAsyncOperation *) (op->getPImpl()->pAsyncOperation);
    return ((SKSimpleTimeoutController*)pImpl)->getMaxAttempts(pOp);
}

int MSimpleTimeoutController::getRelativeTimeoutMillisForAttempt(MAsyncOperation ^ op, int attemptIndex){
    SKAsyncOperation * pOp = (SKAsyncOperation *) (op->getPImpl()->pAsyncOperation);
    return ((SKSimpleTimeoutController*)pImpl)->getRelativeTimeoutMillisForAttempt(pOp, attemptIndex);
}

int MSimpleTimeoutController::getMaxRelativeTimeoutMillis(MAsyncOperation ^ op){
    SKAsyncOperation * pOp = (SKAsyncOperation *) (op->getPImpl()->pAsyncOperation);
    return ((SKSimpleTimeoutController*)pImpl)->getMaxRelativeTimeoutMillis(pOp);
}

String ^ MSimpleTimeoutController::toString(){
    std::string stdstr =  ((SKSimpleTimeoutController*)pImpl)->toString();
    String ^ str = gcnew String( stdstr.c_str(), 0, stdstr.size() );
    return str;
}

MSimpleTimeoutController ^ MSimpleTimeoutController::maxAttempts(int maxAttempts){
    ((SKSimpleTimeoutController*)pImpl)->maxAttempts(maxAttempts);
    return this;
}
MSimpleTimeoutController ^ MSimpleTimeoutController::maxRelativeTimeoutMillis(int maxRelativeTimeoutMillis){
    ((SKSimpleTimeoutController*)pImpl)->maxRelativeTimeoutMillis(maxRelativeTimeoutMillis);
    return this;
}

}