#pragma once

#include "MBaseNSPerspective.h"
using namespace System;
using namespace System::Collections;
using namespace System::Collections::Generic;

namespace SKManagedClient {

    ref class MAsyncPut;
    ref class MAsyncSnapshot;
    ref class MPutOptions;
    ref class MAsyncSyncRequest;

    public interface class MAsyncWritableNSPerspective : public MBaseNSPerspective
    {
    public:
        virtual MAsyncPut ^ put(Dictionary<String ^ , String ^ > ^ dhtValues);
        virtual MAsyncPut ^ put(Dictionary<String ^ , String ^ > ^ dhtValues, MPutOptions ^ putOptions);
        virtual MAsyncPut ^ put(String ^ key, String ^ value, MPutOptions ^ putOptions);
        virtual MAsyncPut ^ put(String ^ key, String ^ value);
        virtual MAsyncSnapshot ^ snapshot();
        virtual MAsyncSnapshot ^ snapshot(Int64 version);
        virtual MAsyncSyncRequest ^ syncRequest();
        virtual MAsyncSyncRequest ^ syncRequest(Int64 version);

    };

}

