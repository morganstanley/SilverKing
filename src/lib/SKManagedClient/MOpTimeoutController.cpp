#include "StdAfx.h"
#include "MOpTimeoutController.h"
#include "SKAsyncOperation.h"

#include <cstddef>
#include "SKOpTimeoutController.h"
#include "MAsyncOperation.h"

using namespace System;
using namespace System::Net;
using namespace System::Runtime::InteropServices;

namespace SKManagedClient {

MOpTimeoutController::~MOpTimeoutController()
{
	this->!MOpTimeoutController();
}

MOpTimeoutController::!MOpTimeoutController()
{
	if(pImpl)
	{
		delete (SKOpTimeoutController*)pImpl ;
		pImpl = NULL;
	}
}

//MOpTimeoutController::MOpTimeoutController() : pImpl(NULL) {}

SKOpTimeoutController_M ^ MOpTimeoutController::getPImpl(){
	SKOpTimeoutController_M ^ opc = gcnew SKOpTimeoutController_M;
	opc->pOpTimeoutController = pImpl;
	return opc;
}

int MOpTimeoutController::getMaxAttempts(MAsyncOperation ^ op){
	SKAsyncOperation * pOp = (SKAsyncOperation *) (op->getPImpl()->pAsyncOperation);
	return ((SKOpTimeoutController*)pImpl)->getMaxAttempts(pOp);
}

int MOpTimeoutController::getRelativeTimeoutMillisForAttempt(MAsyncOperation ^ op, int attemptIndex){
	SKAsyncOperation * pOp = (SKAsyncOperation *) (op->getPImpl()->pAsyncOperation);
	return ((SKOpTimeoutController*)pImpl)->getRelativeTimeoutMillisForAttempt(pOp, attemptIndex);
}

int MOpTimeoutController::getMaxRelativeTimeoutMillis(MAsyncOperation ^ op){
	SKAsyncOperation * pOp = (SKAsyncOperation *) (op->getPImpl()->pAsyncOperation);
	return ((SKOpTimeoutController*)pImpl)->getMaxRelativeTimeoutMillis(pOp);
}


}
