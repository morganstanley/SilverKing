#pragma once

using namespace System;

namespace SKManagedClient {

    ref class SKVersionProvider_M 
    {
    public:
        void * pVersionProvider; // (SKVersionProvider*) //
    };

    public ref class MVersionProvider
    {
    public:
        virtual Int64 getVersion();

        virtual ~MVersionProvider();
        !MVersionProvider();
    internal:
        virtual SKVersionProvider_M ^ getPImpl();
        MVersionProvider(SKVersionProvider_M ^ verProvider);

    protected:
        MVersionProvider();
        void * pImpl;

    };

}