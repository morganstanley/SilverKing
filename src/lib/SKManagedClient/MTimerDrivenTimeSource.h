#pragma once
#include "MRelNanosAbsMillisTimeSource.h"

using namespace System;

namespace SKManagedClient {

    ref class SKTimerDrivenTimeSource_M 
    {
    public: 
        void * pTimeSource; // (SKTimerDrivenTimeSource*) //
    };

    public ref class MTimerDrivenTimeSource : public MRelNanosAbsMillisTimeSource
    {
    public:
        virtual ~MTimerDrivenTimeSource();
        !MTimerDrivenTimeSource();
        MTimerDrivenTimeSource();
        MTimerDrivenTimeSource(Int64 periodMillis);

        /*
        virtual Int64 absTimeMillis();
        virtual int relTimeRemaining(Int64 absDeadlineMillis);
        virtual Int64 relTimeNanos();
        */
        void run();
        void stop();

    internal:
        MTimerDrivenTimeSource(SKTimerDrivenTimeSource_M ^ timeSourceImpl);
        //virtual SKTimerDrivenTimeSource_M ^ getPImpl();

    };

}