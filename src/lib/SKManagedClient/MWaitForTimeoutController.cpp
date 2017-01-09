#include "StdAfx.h"
#include "MWaitForTimeoutController.h"
#include "SKAsyncOperation.h"

#include <cstddef>
#include "SKWaitForTimeoutController.h"
#include "MAsyncOperation.h"

using namespace System;
using namespace System::Net;
using namespace System::Runtime::InteropServices;

namespace SKManagedClient {

MWaitForTimeoutController::~MWaitForTimeoutController()
{
	this->!MWaitForTimeoutController();
}

MWaitForTimeoutController::!MWaitForTimeoutController()
{
	if(pImpl)
	{
		delete (SKWaitForTimeoutController*)pImpl ;
		pImpl = NULL;
	}
}

MWaitForTimeoutController::MWaitForTimeoutController() {
	SKWaitForTimeoutController * pOpCtrl = new SKWaitForTimeoutController();
	pImpl = pOpCtrl;
}
MWaitForTimeoutController::MWaitForTimeoutController(int internalRetryIntervalSeconds) {
	SKWaitForTimeoutController * pOpCtrl = new SKWaitForTimeoutController(internalRetryIntervalSeconds);
	pImpl = pOpCtrl;
}

//internal methods
MWaitForTimeoutController::MWaitForTimeoutController(SKOpTimeoutController_M ^ opController) {
	pImpl = opController->pOpTimeoutController; //(SKWaitForTimeoutController *)
}

SKOpTimeoutController_M ^ MWaitForTimeoutController::getPImpl(){
	SKOpTimeoutController_M ^ opc = gcnew SKOpTimeoutController_M;
	opc->pOpTimeoutController = pImpl;
	return opc;
}

// public methods
int MWaitForTimeoutController::getMaxAttempts(MAsyncOperation ^ op){
	SKAsyncOperation * pOp = (SKAsyncOperation *) (op->getPImpl()->pAsyncOperation);
	return ((SKWaitForTimeoutController*)pImpl)->getMaxAttempts(pOp);
}

int MWaitForTimeoutController::getRelativeTimeoutMillisForAttempt(MAsyncOperation ^ op, int attemptIndex){
	SKAsyncOperation * pOp = (SKAsyncOperation *) (op->getPImpl()->pAsyncOperation);
	return ((SKWaitForTimeoutController*)pImpl)->getRelativeTimeoutMillisForAttempt(pOp, attemptIndex);
}

int MWaitForTimeoutController::getMaxRelativeTimeoutMillis(MAsyncOperation ^ op){
	SKAsyncOperation * pOp = (SKAsyncOperation *) (op->getPImpl()->pAsyncOperation);
	return ((SKWaitForTimeoutController*)pImpl)->getMaxRelativeTimeoutMillis(pOp);
}

String ^ MWaitForTimeoutController::toString(){
	std::string stdstr =  ((SKWaitForTimeoutController*)pImpl)->toString();
	String ^ str = gcnew String( stdstr.c_str(), 0, stdstr.size() );
	return str;
}

}