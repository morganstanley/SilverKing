#pragma once
using namespace System;

namespace SKManagedClient {

    ref class MNamespaceOptions;
    ref class MNamespacePerspectiveOptions;
    ref class MSyncNSPerspective;
    ref class MAsyncNSPerspective;

    ref class SKNamespace_M 
    {
    public:
        void * pNamespace; // (SKNamespace*) //
    };


    public ref class MNamespace
    {
    public:
        MNamespacePerspectiveOptions ^ getDefaultNSPOptions();
        MNamespaceOptions ^ getOptions();
        MAsyncNSPerspective ^ openAsyncPerspective(MNamespacePerspectiveOptions ^ nspOptions);
        MSyncNSPerspective ^ openSyncPerspective(MNamespacePerspectiveOptions ^ nspOptions);
        String ^ getName(); 
        MNamespace ^ clone(String ^ name);                //method for Non-Versioned (WRITE_ONCE)namespaces
        MNamespace ^ clone(String ^ name, Int64 version); //method for Versioned namespaces
        void linkTo(String ^ target);                      //solely for the ifusion team

        !MNamespace();
        virtual ~MNamespace();

    internal:
        MNamespace(SKNamespace_M  ^ nameSpace);
        SKNamespace_M ^ getPImpl(); 

    private:
        void * pImpl;

    };


}

