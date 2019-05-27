#pragma once

#include "SKManagedClient.h"
#include "MStopwatch.h"

using namespace System;

namespace SKManagedClient {

    public ref class MStopwatchBase abstract: public MStopwatch
    {
    public:
        // control
        virtual void start();
        virtual void stop();
        virtual void reset();

        // elapsed - The stopwatch Must be STOPPED when calling these methods!
        virtual Int64 getElapsedNanos();
        virtual Int64 getElapsedMillisLong();
        virtual int getElapsedMillis();
        virtual double getElapsedSeconds();

        // split
        virtual Int64 getSplitNanos();
        virtual Int64 getSplitMillisLong();
        virtual int getSplitMillis();
        virtual double getSplitSeconds();
        
        // misc.
        virtual String ^ getName();
        virtual StopwatchState_M getState();
        virtual String ^ toStringElapsed();
        virtual String ^ toStringSplit();

        virtual String ^ toString();

    private:
        static const double        nanosPerSecond = 1000000000.0;
        //static const BigDecimal    nanosPerSecondBD = new BigDecimal(nanosPerSecond);
        static const long        millisPerSecond = 1000000;

        long    startTimeNanos;
        long    stopTimeNanos;

    protected:

        void * pImpl;

    };

}

