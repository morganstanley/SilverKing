#pragma once

#include "SKManagedClient.h"
#include "MAsyncKeyedOperation.h"

using namespace System;

namespace SKManagedClient {

    public ref class MAsyncPut : public MAsyncKeyedOperation
    {
    public:
        virtual ~MAsyncPut(void);
        !MAsyncPut(void);

    internal:
        MAsyncPut(SKAsyncOperation_M ^ asyncPut);
        virtual SKAsyncOperation_M ^ getPImpl() override;

    };

}