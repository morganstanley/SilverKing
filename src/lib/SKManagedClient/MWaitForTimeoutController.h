#pragma once

#include "MOpTimeoutController.h"

using namespace System;

namespace SKManagedClient {

    public ref class MWaitForTimeoutController : public MOpTimeoutController
    {
    public:
        virtual ~MWaitForTimeoutController();
        !MWaitForTimeoutController();
        MWaitForTimeoutController();
        MWaitForTimeoutController(int internalRetryIntervalSeconds);

        virtual int getMaxAttempts(MAsyncOperation ^ op) override;
        virtual int getRelativeTimeoutMillisForAttempt(MAsyncOperation ^ op, int attemptIndex) override;
        virtual int getMaxRelativeTimeoutMillis(MAsyncOperation ^ op) override;
        virtual String ^ toString() override;

    internal:
        SKOpTimeoutController_M ^ getPImpl();
        MWaitForTimeoutController(SKOpTimeoutController_M ^ pOpImpl);
    //protected: void * pImpl;
    };



}
