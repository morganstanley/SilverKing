#pragma once

#include "MAbsMillisTimeSource.h"
#include "MRelNanosTimeSource.h"

using namespace System;

namespace SKManagedClient {

    ref class SKRelNanosAbsMillisTimeSource_M 
    {
    internal:
        void * pTimeSource; // (SKRelNanosAbsMillisTimeSource*) //
    };


    public ref class MRelNanosAbsMillisTimeSource : public MAbsMillisTimeSource , public MRelNanosTimeSource
    {
    public:
        virtual Int64 relTimeNanos();

        !MRelNanosAbsMillisTimeSource();
        virtual ~MRelNanosAbsMillisTimeSource();

    internal:
        MRelNanosAbsMillisTimeSource(SKRelNanosAbsMillisTimeSource_M ^ timeSource);

    protected:
        MRelNanosAbsMillisTimeSource();
    };

}