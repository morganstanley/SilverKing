#pragma once

#include "MOpTimeoutController.h"

using namespace System;

namespace SKManagedClient {

	public ref class MSimpleTimeoutController : public MOpTimeoutController
	{
	public:
		static MSimpleTimeoutController ^ parse(String ^ def); 
		virtual ~MSimpleTimeoutController();
		!MSimpleTimeoutController();
		MSimpleTimeoutController(int maxAttempts, int maxRelativeTimeoutMillis);

		MSimpleTimeoutController ^ maxAttempts(int maxAttempts);
		MSimpleTimeoutController ^ maxRelativeTimeoutMillis(int maxRelativeTimeoutMillis);
		virtual int getMaxAttempts(MAsyncOperation ^ op) override;
		virtual int getRelativeTimeoutMillisForAttempt(MAsyncOperation ^ op, int attemptIndex) override;
		virtual int getMaxRelativeTimeoutMillis(MAsyncOperation ^ op) override;
		virtual String ^ toString() override;

	internal:
		SKOpTimeoutController_M ^ getPImpl();
		MSimpleTimeoutController(SKOpTimeoutController_M ^ pOpImpl);
	//protected: void * pImpl;
	};



}
