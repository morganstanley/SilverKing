#include "StdAfx.h"
#include "MDHTClient.h"
#include "MSession.h"
#include "MSessionOptions.h"
#include "MValueCreator.h"
#include "MGridConfiguration.h"
#include "MClientDHTConfigurationProvider.h"
#include "MClientDHTConfiguration.h"

#include <string>

#include "skconstants.h"
#include "SKClient.h"
#include "SKSession.h"
#include "SKSessionOptions.h"
#include "SKValueCreator.h"
#include "SKClientDHTConfigurationProvider.h"
#include "SKGridConfiguration.h"
#include "SKClientDHTConfiguration.h"

using namespace std;


#using <mscorlib.dll>
using namespace System;
using namespace System::Collections;

using namespace System::Runtime::InteropServices;
using namespace System::Reflection;
using namespace System::Threading;
using namespace System::IO;


namespace SKManagedClient {


MDHTClient::!MDHTClient()
{
	if(pImpl)
	{
		delete ((SKClient  *)pImpl); 
		pImpl = NULL;
	}

}

MDHTClient::~MDHTClient()
{
	this->!MDHTClient();
}


//new --------------------------------
bool MDHTClient::init(LoggingLevel_M level, String ^ jvmOption)
{
    System::Type ^ t = System::Type::GetType( "SKManagedClient.MDHTClient" );
	bool initialized = false;
    try
    {
        Monitor::Enter(t);

		if(jvmOption != nullptr) {
			char* pJvmOpts =  NULL;
			try {
				pJvmOpts = (char*)(void*)Marshal::StringToHGlobalAnsi(jvmOption);
				initialized = SKClient::init( (LoggingLevel)level,  pJvmOpts );
			} finally {
				Marshal::FreeHGlobal(System::IntPtr(pJvmOpts));
			}
		}
		else {
			initialized = SKClient::init( (LoggingLevel)level );
		}

    }
    finally
    {
        Monitor::Exit(t);
    }
    return initialized;
}

MDHTClient::MDHTClient()
{
	if(!pImpl) 
	{ 
		pImpl = SKClient::getClient ( );
	}
}

MDHTClient ^ MDHTClient::Instance()
{
    System::Type ^ t = System::Type::GetType( "SKManagedClient.MDHTClient" );
    try
    {
        Monitor::Enter(t);

        if (_instance == nullptr)
        {
            _instance = gcnew MDHTClient( );
        }
    }
    finally
    {
        Monitor::Exit(t);
    }
    return _instance;
}
// end of new --------------------------------


// old --------------------------------
MDHTClient::MDHTClient(LoggingLevel_M level, String ^ jvmOption)
{
	pImpl = nullptr;
	if(!pImpl) 
	{ 
		if(jvmOption != nullptr) {
			char* pJvmOpts =  NULL;
			try {
				pJvmOpts = (char*)(void*)Marshal::StringToHGlobalAnsi(jvmOption);
				pImpl = SKClient::getClient ( 
						(LoggingLevel)level,  
						pJvmOpts
				);
			} finally {
				Marshal::FreeHGlobal(System::IntPtr(pJvmOpts));
			}
		}
		else {
			pImpl = SKClient::getClient ( (LoggingLevel)level );
		}
	}
}

MDHTClient ^ MDHTClient::Instance(LoggingLevel_M level, String ^ jvmOption)
{
    System::Type ^ t = System::Type::GetType( "SKManagedClient.MDHTClient" );
    try
    {
        Monitor::Enter(t);

        if (_instance == nullptr)
        {
            _instance = gcnew MDHTClient( level, jvmOption );
        }
    }
    finally
    {
        Monitor::Exit(t);
    }
    return _instance;
}
// end of old --------------------------------


MSession ^ MDHTClient::openSession(MGridConfiguration ^ gridConf) {
	SKGridConfiguration * pGridConfig = (SKGridConfiguration*)(gridConf->getPImpl()->pGridConfig);
	SKSession * pSession = ((SKClient  *)pImpl)->openSession(pGridConfig);
	SKSession_M ^ session_m = gcnew SKSession_M;
	session_m->pSkSession = pSession;
	MSession ^ session = gcnew MSession( session_m );
	return session;
}

/*
MSession ^ MDHTClient::openSession(MClientDHTConfigurationProvider ^ dhtConfigProvider) {
	SKClientDHTConfigurationProvider * clientDHTConfigurationProvider = (SKClientDHTConfigurationProvider*)(dhtConfigProvider->getPImpl()->pConfProvider);
	SKSession * pSession = ((SKClient  *)pImpl)->openSession(clientDHTConfigurationProvider);
	SKSession_M ^ session_m = gcnew SKSession_M;
	session_m->pSkSession = pSession;
	MSession ^ session = gcnew MSession( session_m );
	return session;
}
*/
MSession ^ MDHTClient::openSession(MClientDHTConfiguration ^ dhtConfig) {
	SKClientDHTConfigurationProvider * clientDHTConfiguration = (SKClientDHTConfiguration*)(dhtConfig->getPImpl()->pDhtConfig);
	SKSession * pSession = ((SKClient  *)pImpl)->openSession(clientDHTConfiguration);
	SKSession_M ^ session_m = gcnew SKSession_M;
	session_m->pSkSession = pSession;
	MSession ^ session = gcnew MSession( session_m );
	return session;
}


MSession ^ MDHTClient::openSession(MGridConfiguration ^ gridConf, String ^ preferredServer) {
	char* pPreferredServer =  NULL;
	MSession ^ session = nullptr;
	try { 
		SKGridConfiguration * pGridConfig = (SKGridConfiguration*)(gridConf->getPImpl()->pGridConfig);
		pPreferredServer = (char*)(void*)Marshal::StringToHGlobalAnsi(preferredServer);
		SKSession * pSession = ((SKClient  *)pImpl)->openSession(pGridConfig, pPreferredServer);
		SKSession_M ^ session_m = gcnew SKSession_M;
		session_m->pSkSession = pSession;
		session = gcnew MSession( session_m );
	}
	finally {
		Marshal::FreeHGlobal(System::IntPtr(pPreferredServer));
	}
	return session;
}

MSession ^ MDHTClient::openSession(MSessionOptions ^ sessionOptions) {
	SKSessionOptions * pSessionOptions = (SKSessionOptions*)(sessionOptions->getPImpl()->pSessOptions);
	SKSession * pSession = ((SKClient  *)pImpl)->openSession(pSessionOptions);

	SKSession_M ^ session_m = gcnew SKSession_M;
	session_m->pSkSession  = (void*)pSession ;
	MSession ^ session = gcnew MSession( session_m );
	return session;
}


MValueCreator ^ MDHTClient::getValueCreator() {
	SKValueCreator * pValueCreator = SKClient::getValueCreator();
	SKValueCreator_M ^ valueCreator_m = gcnew SKValueCreator_M;
	valueCreator_m->pValueCreator = pValueCreator;
	return gcnew MValueCreator(valueCreator_m);
}



}
