#include "StdAfx.h"
#include "MSessionEstablishmentTimeoutController.h"
#include "MSessionOptions.h"

#include <cstddef>
#include "SKSessionEstablishmentTimeoutController.h"
#include "SKSessionOptions.h"

using namespace System;
using namespace System::Net;
using namespace System::Runtime::InteropServices;

namespace SKManagedClient {

MSessionEstablishmentTimeoutController::~MSessionEstablishmentTimeoutController()
{
	this->!MSessionEstablishmentTimeoutController();
}

MSessionEstablishmentTimeoutController::!MSessionEstablishmentTimeoutController()
{
	if(pImpl)
	{
		delete (SKSessionEstablishmentTimeoutController*)pImpl ;
		pImpl = NULL;
	}
}

MSessionEstablishmentTimeoutController::MSessionEstablishmentTimeoutController() : pImpl(NULL) {}

SKSessionEstablishmentTimeoutController_M ^ MSessionEstablishmentTimeoutController::getPImpl(){
	SKSessionEstablishmentTimeoutController_M ^ setc = gcnew SKSessionEstablishmentTimeoutController_M;
	setc->pSessionEstablishmentTimeoutController = pImpl;
	return setc;
}

int MSessionEstablishmentTimeoutController::getMaxAttempts(MSessionOptions ^ sessOpt){
	SKSessionOptions * pSessOpt = (SKSessionOptions *) (sessOpt->getPImpl()->pSessOptions);
	return ((SKSessionEstablishmentTimeoutController*)pImpl)->getMaxAttempts(pSessOpt);
}

int MSessionEstablishmentTimeoutController::getRelativeTimeoutMillisForAttempt(MSessionOptions ^ sessOpt, int attemptIndex){
	SKSessionOptions * pSessOpt = (SKSessionOptions *) (sessOpt->getPImpl()->pSessOptions);
	return ((SKSessionEstablishmentTimeoutController*)pImpl)->getRelativeTimeoutMillisForAttempt(pSessOpt, attemptIndex);
}

int MSessionEstablishmentTimeoutController::getMaxRelativeTimeoutMillis(MSessionOptions ^ sessOpt){
	SKSessionOptions * pSessOpt = (SKSessionOptions *) (sessOpt->getPImpl()->pSessOptions);
	return ((SKSessionEstablishmentTimeoutController*)pImpl)->getMaxRelativeTimeoutMillis(pSessOpt);
}

String ^ MSessionEstablishmentTimeoutController::toString(){
	std::string stdstr =  ((SKSessionEstablishmentTimeoutController*)pImpl)->toString();
	String ^ str = gcnew String( stdstr.c_str(), 0, stdstr.size() );
	return str;
}

MSessionEstablishmentTimeoutController::MSessionEstablishmentTimeoutController(SKSessionEstablishmentTimeoutController_M ^ sessController){
	pImpl = sessController->pSessionEstablishmentTimeoutController; //(SKSessionEstablishmentTimeoutController *)
}

}
