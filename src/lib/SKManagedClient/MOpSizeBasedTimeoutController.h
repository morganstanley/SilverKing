#pragma once

#include "MOpTimeoutController.h"

using namespace System;

namespace SKManagedClient {

	public ref class MOpSizeBasedTimeoutController : public MOpTimeoutController
	{
	public:
		static MOpSizeBasedTimeoutController ^ parse(String ^ def); 
		virtual ~MOpSizeBasedTimeoutController();
		!MOpSizeBasedTimeoutController();
		MOpSizeBasedTimeoutController();
		MOpSizeBasedTimeoutController(int maxAttempts, int constantTimeMillis, int itemTimeMillis, int maxRelTimeoutMillis);

		MOpSizeBasedTimeoutController ^ itemTimeMillis(int itemTimeMillis);
		MOpSizeBasedTimeoutController ^ constantTimeMillis(int constantTimeMillis);
		MOpSizeBasedTimeoutController ^ maxRelTimeoutMillis(int maxRelTimeoutMillis);
		MOpSizeBasedTimeoutController ^ maxAttempts(int maxAttempts);
		
		virtual int getMaxAttempts(MAsyncOperation ^ op) override;
		virtual int getRelativeTimeoutMillisForAttempt(MAsyncOperation ^ op, int attemptIndex) override;
		virtual int getMaxRelativeTimeoutMillis(MAsyncOperation ^ op) override;
		virtual String ^ toString() override;

	internal:
		SKOpTimeoutController_M ^ getPImpl();
		MOpSizeBasedTimeoutController(SKOpTimeoutController_M ^ pOpImpl);
	//protected: void * pImpl;
	};



}
