#pragma once

#include "MSimpleStopwatch.h"

using namespace System;

namespace SKManagedClient {

    ref class MSimpleNamedStopwatch : public MSimpleStopwatch
    {
    public:

        MSimpleNamedStopwatch();
        MSimpleNamedStopwatch(String ^ name);
        MSimpleNamedStopwatch(MRelNanosAbsMillisTimeSource ^ relNanosTimeSource, String ^ name);
        virtual ~MSimpleNamedStopwatch();
        !MSimpleNamedStopwatch();
    
    };



}
