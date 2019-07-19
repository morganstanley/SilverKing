#pragma once

#include "SKManagedClient.h"

namespace SKManagedClient {

    ref class MClientDHTConfigurationProvider;
    ref class MClientDHTConfiguration;
    ref class MGridConfiguration;
    ref class MSessionOptions;
    ref class MSession;
    ref class MValueCreator;

    ref class SKClient_M
    {
    public:
        void * pClient;
    };


    public ref class MDHTClient
    {
    public:
        //new
        static bool init(LoggingLevel_M level, String ^ jvmOptions);  //init jvm
        static MDHTClient ^ Instance();  //singleton instance
        //old
        static MDHTClient ^ Instance(LoggingLevel_M level, String ^ jvmOptions);  //singleton instance
        
        !MDHTClient();
        ~MDHTClient();

        MSession ^ openSession(MGridConfiguration ^ pGridConf);
        MSession ^ openSession(MGridConfiguration ^ pGridConf, System::String ^ preferredServer);
        MSession ^ openSession(MSessionOptions ^ sessionOptions);

        //FIXME: these 2 below: add classes or convert accordingly
        //MSession ^ openSession(MClientDHTConfigurationProvider ^ dhtConfigProvider);
        MSession ^ openSession(MClientDHTConfiguration ^ dhtConfig);
        static MValueCreator ^ getValueCreator();

    private:
        //new
        MDHTClient();
        //old
        MDHTClient(LoggingLevel_M level, System::String ^ jvmOptions);

        static MDHTClient ^  _instance;
        static void  *  pImpl;

    };


}