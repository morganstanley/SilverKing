#pragma once

#include "SKManagedClient.h"
#include "MAsyncValueRetrieval.h"
#include "MAsyncSingleRetrieval.h"

using namespace System;
using namespace System::Collections;
using namespace System::Collections::Generic;

namespace SKManagedClient {

    ref class SKAsyncSingleValueRetrieval_M 
    {
    public:
        void * pRetrieval;
    };

    public ref class MAsyncSingleValueRetrieval : public MAsyncValueRetrieval/*, public MAsyncSingleRetrieval*/
    {
    public:

        virtual ~MAsyncSingleValueRetrieval(void);
        !MAsyncSingleValueRetrieval(void);

        /**
        * Returns the raw value if it is present. 
        * @return the raw value if it is present
        * @throws RetrievalException
        */
        virtual String ^ getValue();    // throws RetrievalException;    

        //MAsyncSingleRetrieval
        virtual MStoredValue ^ getStoredValue();

    internal:
        explicit MAsyncSingleValueRetrieval(SKAsyncOperation_M ^ retrieval);
        virtual SKAsyncOperation_M ^ getPImpl()  override;

    private:
        void * pImpl;

    };


}