#include "StdAfx.h"
#include "MOpSizeBasedTimeoutController.h"
#include "SKAsyncOperation.h"

#include <cstddef>
#include "SKOpSizeBasedTimeoutController.h"
#include "MAsyncOperation.h"

using namespace System;
using namespace System::Net;
using namespace System::Runtime::InteropServices;

namespace SKManagedClient {

MOpSizeBasedTimeoutController ^ MOpSizeBasedTimeoutController::parse(String ^ def){
	SKOpSizeBasedTimeoutController * ptc = SKOpSizeBasedTimeoutController::parse((char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(def).ToPointer());
	SKOpTimeoutController_M ^ tc = gcnew SKOpTimeoutController_M;
	tc->pOpTimeoutController = ptc;
	return gcnew MOpSizeBasedTimeoutController(tc);

}

MOpSizeBasedTimeoutController::~MOpSizeBasedTimeoutController()
{
	this->!MOpSizeBasedTimeoutController();
}

MOpSizeBasedTimeoutController::!MOpSizeBasedTimeoutController()
{
	if(pImpl)
	{
		delete (SKOpSizeBasedTimeoutController*)pImpl ;
		pImpl = NULL;
	}
}

MOpSizeBasedTimeoutController::MOpSizeBasedTimeoutController() {
	SKOpSizeBasedTimeoutController * pOpCtrl = new SKOpSizeBasedTimeoutController();
	pImpl = pOpCtrl;
}
MOpSizeBasedTimeoutController::MOpSizeBasedTimeoutController(int maxAttempts, int constantTimeMillis, 
								int itemTimeMillis, int maxRelTimeoutMillis) 
{
	SKOpSizeBasedTimeoutController * pOpCtrl = new SKOpSizeBasedTimeoutController(
								maxAttempts, constantTimeMillis, itemTimeMillis, maxRelTimeoutMillis );
	pImpl = pOpCtrl;
}

//internal methods
MOpSizeBasedTimeoutController::MOpSizeBasedTimeoutController(SKOpTimeoutController_M ^ opController) {
	pImpl = opController->pOpTimeoutController; //(SKOpSizeBasedTimeoutController *)
}

SKOpTimeoutController_M ^ MOpSizeBasedTimeoutController::getPImpl(){
	SKOpTimeoutController_M ^ opc = gcnew SKOpTimeoutController_M;
	opc->pOpTimeoutController = pImpl;
	return opc;
}

// public methods
int MOpSizeBasedTimeoutController::getMaxAttempts(MAsyncOperation ^ op){
	SKAsyncOperation * pOp = (SKAsyncOperation *) (op->getPImpl()->pAsyncOperation);
	return ((SKOpSizeBasedTimeoutController*)pImpl)->getMaxAttempts(pOp);
}

int MOpSizeBasedTimeoutController::getRelativeTimeoutMillisForAttempt(MAsyncOperation ^ op, int attemptIndex){
	SKAsyncOperation * pOp = (SKAsyncOperation *) (op->getPImpl()->pAsyncOperation);
	return ((SKOpSizeBasedTimeoutController*)pImpl)->getRelativeTimeoutMillisForAttempt(pOp, attemptIndex);
}

int MOpSizeBasedTimeoutController::getMaxRelativeTimeoutMillis(MAsyncOperation ^ op){
	SKAsyncOperation * pOp = (SKAsyncOperation *) (op->getPImpl()->pAsyncOperation);
	return ((SKOpSizeBasedTimeoutController*)pImpl)->getMaxRelativeTimeoutMillis(pOp);
}

String ^ MOpSizeBasedTimeoutController::toString(){
	std::string stdstr =  ((SKOpSizeBasedTimeoutController*)pImpl)->toString();
	String ^ str = gcnew String( stdstr.c_str(), 0, stdstr.size() );
	return str;
}

MOpSizeBasedTimeoutController ^ MOpSizeBasedTimeoutController::maxAttempts(int maxAttempts){
	((SKOpSizeBasedTimeoutController*)pImpl)->maxAttempts(maxAttempts);
	return this;
}

MOpSizeBasedTimeoutController ^ MOpSizeBasedTimeoutController::itemTimeMillis(int itemTimeMillis){
	((SKOpSizeBasedTimeoutController*)pImpl)->itemTimeMillis(itemTimeMillis);
	return this;
}

MOpSizeBasedTimeoutController ^ MOpSizeBasedTimeoutController::constantTimeMillis(int constantTimeMillis){
	((SKOpSizeBasedTimeoutController*)pImpl)->constantTimeMillis(constantTimeMillis);
	return this;
}

MOpSizeBasedTimeoutController ^ MOpSizeBasedTimeoutController::maxRelTimeoutMillis(int maxRelTimeoutMillis){
	((SKOpSizeBasedTimeoutController*)pImpl)->maxRelTimeoutMillis(maxRelTimeoutMillis);
	return this;
}


}