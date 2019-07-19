#pragma once

using namespace System;

namespace SKManagedClient {

    ref class MClientDHTConfiguration;
    ref class MSessionEstablishmentTimeoutController;
    ref class MSimpleSessionEstablishmentTimeoutController;

    ref class SKSessionOptions_M 
    {
    public:
        void * pSessOptions;
    };

    public ref class MSessionOptions 
    {
    public:
        static MSessionEstablishmentTimeoutController ^ getDefaultTimeoutController();
        MSessionOptions(MClientDHTConfiguration ^ dhtConfig);
        MSessionOptions(MClientDHTConfiguration ^ dhtConfig, String ^ preferredServer);
        MSessionOptions(MClientDHTConfiguration ^ dhtConfig, String ^ preferredServer,
               MSessionEstablishmentTimeoutController ^ timeoutController);
        !MSessionOptions();
        virtual ~MSessionOptions();

        void setDefaultTimeoutController(MSessionEstablishmentTimeoutController ^ defaultTimeoutController);
        MSessionEstablishmentTimeoutController ^ getTimeoutController();
        MClientDHTConfiguration ^ getDHTConfig();
        String ^ getPreferredServer();
        String ^ toString();

    internal:
        MSessionOptions(SKSessionOptions_M ^ pSesOptions);
        SKSessionOptions_M ^ getPImpl();

    private:
        void * pImpl;
    };

}

