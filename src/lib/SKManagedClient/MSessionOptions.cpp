#include "StdAfx.h"
#include "MSessionOptions.h"
#include "MClientDHTConfiguration.h"
#include "MSessionEstablishmentTimeoutController.h"

#include <stdlib.h> 
#include "SKSessionOptions.h"
#include "SKClientDHTConfiguration.h"
#include "SKSessionEstablishmentTimeoutController.h"

using namespace System::Runtime::InteropServices;

namespace SKManagedClient {

// static method
MSessionEstablishmentTimeoutController ^ MSessionOptions::getDefaultTimeoutController() {
	SKSessionEstablishmentTimeoutController * pSetc = SKSessionOptions::getDefaultTimeoutController();
	SKSessionEstablishmentTimeoutController_M ^ sessCtrlImp = gcnew SKSessionEstablishmentTimeoutController_M;
	sessCtrlImp->pSessionEstablishmentTimeoutController = pSetc;
	return gcnew MSessionEstablishmentTimeoutController(sessCtrlImp);
}

MSessionOptions::~MSessionOptions()
{
	this->!MSessionOptions();
}

MSessionOptions::!MSessionOptions(){
	if(pImpl)
	{
		delete (SKSessionOptions*) pImpl; 
		pImpl = NULL;
	}
}

//c-tors
MSessionOptions::MSessionOptions(SKSessionOptions_M ^ sessionOptions) 
{
	pImpl = sessionOptions->pSessOptions;
}

SKSessionOptions_M ^ MSessionOptions::getPImpl() {
	SKSessionOptions_M ^ so = gcnew SKSessionOptions_M;
	so->pSessOptions = pImpl;
	return so;
}

MSessionOptions::MSessionOptions(MClientDHTConfiguration ^ dhtConfig){
	SKClientDHTConfiguration* pDhtConf = (SKClientDHTConfiguration*) (dhtConfig->getPImpl()->pDhtConfig);
	pImpl = new SKSessionOptions(pDhtConf);
}

MSessionOptions::MSessionOptions(MClientDHTConfiguration ^ dhtConfig, String ^ preferredServer){
	char * pPreferredServer = NULL;
	try {

   	    pPreferredServer = (char*)(void*)Marshal::StringToHGlobalAnsi(preferredServer);
		SKClientDHTConfiguration* pDhtConf = (SKClientDHTConfiguration*) (dhtConfig->getPImpl()->pDhtConfig);
		pImpl = new SKSessionOptions(pDhtConf, pPreferredServer);
	} 
	finally {
			Marshal::FreeHGlobal(System::IntPtr(pPreferredServer ));
	}
}

MSessionOptions::MSessionOptions(MClientDHTConfiguration ^ dhtConfig, String ^ preferredServer, MSessionEstablishmentTimeoutController ^ timeoutController)
{
	char * pPreferredServer = NULL;
	try {

   	    pPreferredServer = (char*)(void*)Marshal::StringToHGlobalAnsi(preferredServer);
		SKClientDHTConfiguration* pDhtConf = (SKClientDHTConfiguration*) (dhtConfig->getPImpl()->pDhtConfig);
		SKSessionEstablishmentTimeoutController* pSessCtrl = (SKSessionEstablishmentTimeoutController*) (timeoutController->getPImpl()->pSessionEstablishmentTimeoutController);
		pImpl = new SKSessionOptions(pDhtConf, pPreferredServer, pSessCtrl);
	} 
	finally {
			Marshal::FreeHGlobal(System::IntPtr(pPreferredServer ));
	}
}

//public methods
void MSessionOptions::setDefaultTimeoutController(MSessionEstablishmentTimeoutController ^ defaultTimeoutController)
{
	((SKSessionOptions*)pImpl)->setDefaultTimeoutController( (SKSessionEstablishmentTimeoutController*)(defaultTimeoutController->getPImpl()->pSessionEstablishmentTimeoutController));
}

MSessionEstablishmentTimeoutController ^ MSessionOptions::getTimeoutController()
{
	SKSessionEstablishmentTimeoutController * pSessCtrl = ((SKSessionOptions*)pImpl)->getTimeoutController();
	SKSessionEstablishmentTimeoutController_M ^ sessTimeoutCtrl = gcnew SKSessionEstablishmentTimeoutController_M;
	sessTimeoutCtrl->pSessionEstablishmentTimeoutController = pSessCtrl;
	return gcnew MSessionEstablishmentTimeoutController(sessTimeoutCtrl);
}

MClientDHTConfiguration ^ MSessionOptions::getDHTConfig(){
	SKClientDHTConfiguration * pDhtConf = ((SKSessionOptions*)pImpl)->getDHTConfig();
	SKClientDHTConfiguration_M ^ conf = gcnew SKClientDHTConfiguration_M;
	conf->pDhtConfig = pDhtConf;
	return gcnew MClientDHTConfiguration(conf);
}

System::String ^ MSessionOptions::getPreferredServer(){
	char * pPrefServer = ((SKSessionOptions*)pImpl)->getPreferredServer();
	System::String ^ server = gcnew System::String(pPrefServer);
	delete pPrefServer;
	return server;
}

System::String ^ MSessionOptions::toString(){
	char * pSessInfo = ((SKSessionOptions*)pImpl)->toString();
	System::String ^ sessionString = gcnew System::String(pSessInfo);
	free( pSessInfo );
	return sessionString;
}



}

