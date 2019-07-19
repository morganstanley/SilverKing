#pragma once

#include "MSessionEstablishmentTimeoutController.h"

using namespace System;

namespace SKManagedClient {

    public ref class MSimpleSessionEstablishmentTimeoutController : public MSessionEstablishmentTimeoutController
    {
    public:
        static MSimpleSessionEstablishmentTimeoutController ^ parse(String ^ def); 
        virtual ~MSimpleSessionEstablishmentTimeoutController();
        !MSimpleSessionEstablishmentTimeoutController();
        MSimpleSessionEstablishmentTimeoutController(int maxAttempts, int attemptRelativeTimeoutMillis, int maxRelativeTimeoutMillis);

        MSimpleSessionEstablishmentTimeoutController ^ attemptRelativeTimeoutMillis(int attemptRelativeTimeoutMillis);
        MSimpleSessionEstablishmentTimeoutController ^ maxAttempts(int maxAttempts);
        MSimpleSessionEstablishmentTimeoutController ^ maxRelativeTimeoutMillis(int maxRelativeTimeoutMillis);
        virtual int getMaxAttempts(MSessionOptions ^ sessOpt) override;
        virtual int getRelativeTimeoutMillisForAttempt(MSessionOptions ^ sessOpt, int attemptIndex) override;
        virtual int getMaxRelativeTimeoutMillis(MSessionOptions ^ sessOpt) override;
        virtual String ^ toString() override;

    internal:
        SKSessionEstablishmentTimeoutController_M ^ getPImpl();
        MSimpleSessionEstablishmentTimeoutController(SKSessionEstablishmentTimeoutController_M ^ pCtrlImpl);
        //MSimpleSessionEstablishmentTimeoutController() ;
    //protected: void * pImpl;
    };



}
