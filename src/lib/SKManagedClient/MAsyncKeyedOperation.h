#pragma once
#include "SKManagedClient.h"
#include "MAsyncOperation.h"

using namespace System;
using namespace System::Collections;
using namespace System::Collections::Generic;

namespace SKManagedClient {

    public ref class MAsyncKeyedOperation abstract : public MAsyncOperation
    {
    public:

        /**
        * Get the set of keys in this operation.
        * @return the set of keys in this operation
        */
        virtual HashSet<String^> ^ getKeys();

        /**
        * Get the set of all incomplete keys in this operation.
        * @return the set of keys in this operation
        */
        virtual HashSet<String^> ^ getIncompleteKeys();

        /**
        * Get the OperationState for the given key.
        * @param key
        * @return OperationState for the given key
        */
        virtual SKOperationState_M getOperationState(String ^ key);

        /**
        * Get the OperationState for all keys in this operation
        * @return the OperationState for all keys in this operation
        */
        virtual Dictionary<String^, SKOperationState_M> ^ getOperationStateMap();

        virtual ~MAsyncKeyedOperation();
        !MAsyncKeyedOperation();
    protected:
        MAsyncKeyedOperation();

    };

}