#pragma once

#include "MBaseNSPerspective.h"
using namespace System;
using namespace System::Collections;
using namespace System::Collections::Generic;

namespace SKManagedClient {

    ref class MAsyncSyncRequest;
    ref class MStoredValue;
    ref class MGetOptions;
    ref class MWaitOptions;

    public interface class MSyncReadableNSPerspective : public MBaseNSPerspective
    {
    public:

        virtual Dictionary<String ^ , String ^ > ^ get(HashSet<String ^> ^ dhtKeys);
        virtual Dictionary<String ^ , MStoredValue ^ > ^ get(HashSet<String ^> ^ dhtKeys, MGetOptions ^ getOptions);
        virtual MStoredValue ^ get(String ^ key, MGetOptions ^ getOptions);
        virtual String ^ get(String ^ key);
 
        virtual Dictionary<String ^ , String ^ > ^ waitFor(HashSet<String ^ > ^ dhtKeys);
        virtual Dictionary<String ^ , MStoredValue ^ > ^ waitFor(HashSet<String ^ > ^ dhtKeys, MWaitOptions ^ waitOptions);
        virtual MStoredValue ^ waitFor(String ^  key, MWaitOptions ^ waitOptions);
        virtual String ^ waitFor(String ^ key);

    };
}

