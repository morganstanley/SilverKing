#pragma once
#include "MRelNanosAbsMillisTimeSource.h"

using namespace System;

namespace SKManagedClient {

    ref class SKSystemTimeSource_M 
    {
    public:
        void * pTimeSource; // (SKSystemTimeSource*) //
    };

    public ref class MSystemTimeSource : public MRelNanosAbsMillisTimeSource
    {
    public:
        virtual ~MSystemTimeSource(void);
        !MSystemTimeSource();
        MSystemTimeSource();

        //virtual Int64 absTimeMillis();
        //virtual int relTimeRemaining(Int64 absDeadlineMillis);
        //virtual Int64 relTimeNanos();

    internal:
        MSystemTimeSource(SKSystemTimeSource_M ^ timeSourceImpl);
        //virtual SKSystemTimeSource_M ^ getPImpl();

    //private:
    //    void * pImpl;
    };

}