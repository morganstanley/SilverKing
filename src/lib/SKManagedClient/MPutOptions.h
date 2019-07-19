#pragma once
#include "SKManagedClient.h"
#include <stdint.h>

using namespace System;
using namespace System::Collections;
using namespace System::Collections::Generic;

namespace SKManagedClient {

    ref class MSecondaryTarget;
    ref class MOpTimeoutController;

    ref class SKPutOptions_M 
    {
    internal:
        void * pPutOptions; // (SKPutOptions*) //
    };

    public ref class MPutOptions
    {
    public:
        //life-time mgmt
        static MPutOptions ^ parse(String ^ def);
        !MPutOptions();
        virtual ~MPutOptions();
        MPutOptions(MOpTimeoutController ^ opTimeoutController, SKCompression_M compression, 
                    SKChecksumType_M checksumType, bool checksumCompressedValues, Int64 version, 
                    HashSet<MSecondaryTarget^> ^ secondaryTargets, String ^ userData);

        //methods
        MPutOptions ^ compression(SKCompression_M compression);
        MPutOptions ^ version(Int64 version);
        MPutOptions ^ userData(String ^ userData);
        MPutOptions ^ checksumType(SKChecksumType_M checksumType);
        bool getChecksumCompressedValues();
        SKCompression_M getCompression();
        SKChecksumType_M getChecksumType();
        Int64 getVersion();
        String ^ getUserData();
        bool equals(Object ^ other);
        String ^ toString();
        MPutOptions ^ opTimeoutController(MOpTimeoutController ^ opTimeoutController);
        MPutOptions ^ secondaryTargets(HashSet<MSecondaryTarget^> ^ secondaryTargets);
        MPutOptions ^ secondaryTargets(MSecondaryTarget ^ secondaryTarget);
        HashSet<MSecondaryTarget^> ^ getSecondaryTargets();
        //MPutOptions ^ checksumCompressedValues(bool checksumCompressedValues );

    internal:
        MPutOptions(SKPutOptions_M ^ pPutOptsImpl);
        SKPutOptions_M ^ getPImpl();

    private:
        void * pImpl;

    };

}