#pragma once
#include "limits.h"

using namespace System;

namespace SKManagedClient {

	ref class MSessionOptions;

	ref class SKSessionEstablishmentTimeoutController_M
	{
	public:
		void * pSessionEstablishmentTimeoutController;  // subclass of (SKSessionEstablishmentTimeoutController*) //
	};

	public ref class MSessionEstablishmentTimeoutController  //abstract
	{
	public:
		virtual ~MSessionEstablishmentTimeoutController();
		!MSessionEstablishmentTimeoutController();

		virtual int getMaxAttempts(MSessionOptions ^ sessOpt);
		virtual int getRelativeTimeoutMillisForAttempt(MSessionOptions ^ sessOpt, int attemptIndex);
		virtual int getMaxRelativeTimeoutMillis(MSessionOptions ^ sessOpt);
		virtual String ^ toString();

	internal:
		SKSessionEstablishmentTimeoutController_M ^ getPImpl();
		MSessionEstablishmentTimeoutController(SKSessionEstablishmentTimeoutController_M ^ sessController); //comment
	protected:
		MSessionEstablishmentTimeoutController(); //
		void * pImpl;
	};



}
