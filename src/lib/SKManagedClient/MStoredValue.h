#pragma once

#include "MMetaData.h"

using namespace System;

namespace SKManagedClient {

    ref class SKStoredValue_M 
    {
    public:
        void * pStoredValue;
    };

    public ref class MStoredValue : public MMetaData
    {
    public:
        virtual ~MStoredValue(void);
        !MStoredValue(void);

        MMetaData ^ getMetaData() ;
        String ^ getValue() ;
        MStoredValue ^ next() ;

    internal:
        MStoredValue(SKStoredValue_M ^ storedValue);
        //SKMetaData_M ^ getPImpl();

    };

}