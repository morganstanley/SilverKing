#pragma once
#include "limits.h"

using namespace System;

namespace SKManagedClient {

	ref class MAsyncOperation;

	ref class SKOpTimeoutController_M
	{
	public:
		void * pOpTimeoutController;  // subclass of (SKOpTimeoutController*) //
	};

	public ref class MOpTimeoutController abstract
	{
	public:
		virtual ~MOpTimeoutController();
		!MOpTimeoutController();

		virtual int getMaxAttempts(MAsyncOperation ^ op);
		virtual int getRelativeTimeoutMillisForAttempt(MAsyncOperation ^ op, int attemptIndex);
		virtual int getMaxRelativeTimeoutMillis(MAsyncOperation ^ op);
		virtual String ^ toString() = 0;
		static const int  INFINITE_REL_TIMEOUT = INT_MAX - 1;

	internal:
		SKOpTimeoutController_M ^ getPImpl();
		//no MOpTimeoutController(SKOpTimeoutController_M ^ pOpImpl);
	protected:
		//MOpTimeoutController();
		void * pImpl;
	};



}
