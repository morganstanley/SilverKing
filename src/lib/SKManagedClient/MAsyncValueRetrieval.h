#pragma once

#include "SKManagedClient.h"
#include "MAsyncRetrieval.h"

using namespace System;
using namespace System::Collections;
using namespace System::Collections::Generic;

namespace SKManagedClient {

    public ref class MAsyncValueRetrieval : public MAsyncRetrieval
    {
    public:

        /**
        * Returns raw values for all successfully complete retrievals. 
        * @return
        * @throws RetrievalException
        */
        virtual Dictionary<String ^ , String ^ > ^ getValues();  //  throws RetrievalException;
        /**
        * Returns the raw value for the given key if it is present. 
        * @return
        * @throws RetrievalException
        */
        virtual  String ^ getValue(String ^ key);    // throws RetrievalException;    
        /**
        * Returns raw values for all successfully complete retrievals that
        * have completed since the last call to this method and getLatestStoredValues(). 
        * Each successfully retrieved value will be reported exactly once by this 
        * method and getLatestStoredValues().
        * This method is unaffected by calls to either getValues() or getStoredValues().
        * Concurrent execution is permitted, but the precise values returned are
        * undefined.  
        * @return
        * @throws RetrievalException
        */
        virtual  Dictionary<String ^ , String ^ > ^ getLatestValues();    // throws RetrievalException;

        virtual ~MAsyncValueRetrieval();
        !MAsyncValueRetrieval();

    internal:
        MAsyncValueRetrieval(SKAsyncOperation_M ^ asyncValueRetrieval);
        virtual SKAsyncOperation_M ^ getPImpl() override;
    protected:
        MAsyncValueRetrieval();
        Dictionary<String ^ , String ^ > ^ retrieveValues(bool latest);

    };

}