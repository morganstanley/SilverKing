#include "StdAfx.h"
#include "MSession.h"
#include "MNamespaceOptions.h"
#include "MNamespace.h"
#include "MNamespaceCreationOptions.h"
#include "MNamespacePerspectiveOptions.h"
#include "MNamespaceOptions.h"
#include "MSyncNSPerspective.h"
#include "MAsyncNSPerspective.h"
#include "MPutOptions.h"
#include "MGetOptions.h"
#include "MWaitOptions.h"

#include "SKSession.h"
#include "SKNamespaceOptions.h"
#include "SKNamespace.h"
#include "SKNamespaceCreationOptions.h"
#include "SKNamespacePerspectiveOptions.h"
#include "SKNamespaceOptions.h"
#include "SKSyncNSPerspective.h"
#include "SKAsyncNSPerspective.h"
#include "SKPutOptions.h"
#include "SKRetrievalOptions.h"
#include "SKGetOptions.h"
#include "SKWaitOptions.h"

namespace SKManagedClient {

	MSession::~MSession() 
	{
		this->!MSession();
	}

	MSession::!MSession() {
		if(pImpl)
		{
			delete (SKSession*) pImpl;
			pImpl = NULL;
		}
	}

	MSession::MSession(SKSession_M ^ skSession_m) {
		pImpl = skSession_m->pSkSession;
	}

	SKSession_M ^ MSession::getPImpl() {
		SKSession_M ^ skSession_m = gcnew SKSession_M();
		skSession_m->pSkSession = pImpl;
		return skSession_m;
	}

	MNamespace ^ MSession::createNamespace(System::String ^ ns, MNamespaceOptions ^ nsOptions) {
		SKNamespaceOptions * pNsOptions = (SKNamespaceOptions *)(nsOptions->getPImpl()->pNsOptions);
		SKNamespace * pNamespace = ((SKSession *)pImpl)->createNamespace(
			(char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(ns).ToPointer(),
			pNsOptions
		);
		SKNamespace_M ^ skNamespace_m = gcnew SKNamespace_M;
		skNamespace_m->pNamespace  = pNamespace;
		MNamespace ^ mskNamespace = gcnew MNamespace(skNamespace_m);
		return mskNamespace;
	}

	MNamespace ^ MSession::createNamespace(System::String ^ ns) {
		SKNamespace * pNamespace = ((SKSession *)pImpl)->createNamespace(
			(char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(ns).ToPointer()
		);
		SKNamespace_M ^ skNamespace_m = gcnew SKNamespace_M;
		skNamespace_m->pNamespace  = pNamespace;
		MNamespace ^ mskNamespace = gcnew MNamespace(skNamespace_m);
		return mskNamespace;
	}

	MNamespace ^ MSession::getNamespace(System::String ^ ns) {
		SKNamespace * pNamespace = ((SKSession *)pImpl)->getNamespace(
			(char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(ns).ToPointer()
		);
		SKNamespace_M ^ skNamespace_m = gcnew SKNamespace_M;
		skNamespace_m->pNamespace  = pNamespace;
		MNamespace ^ mskNamespace = gcnew MNamespace(skNamespace_m);
		return mskNamespace;
	}

	void MSession::deleteNamespace(System::String ^ ns){
		return ((SKSession *)pImpl)->deleteNamespace(
			(char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(ns).ToPointer()
		);
	}

	void MSession::recoverNamespace(System::String ^ ns){
		return ((SKSession *)pImpl)->recoverNamespace(
			(char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(ns).ToPointer()
		);
	}

	MPutOptions ^ MSession::getDefaultPutOptions(){
		SKPutOptions * pPutOptions = ((SKSession *)pImpl)->getDefaultPutOptions();
		SKPutOptions_M ^ putOptions_m = gcnew SKPutOptions_M;
		putOptions_m->pPutOptions  = pPutOptions;
		MPutOptions ^ putOptions = gcnew MPutOptions(putOptions_m);
		return putOptions;
	}

	MGetOptions ^ MSession::getDefaultGetOptions(){
		SKGetOptions * pGetOptions = ((SKSession *)pImpl)->getDefaultGetOptions();
		SKGetOptions_M ^ getOptions_m = gcnew SKGetOptions_M;
		getOptions_m->pGetOptions  = pGetOptions;
		MGetOptions ^ getOptions = gcnew MGetOptions(getOptions_m);
		return getOptions;
	}

	MWaitOptions ^ MSession::getDefaultWaitOptions(){
		SKWaitOptions * pWaitOptions = ((SKSession *)pImpl)->getDefaultWaitOptions();
		SKWaitOptions_M ^ waitOptions_m = gcnew SKWaitOptions_M;
		waitOptions_m->pWaitOptions  = pWaitOptions;
		MWaitOptions ^ waitOptions = gcnew MWaitOptions(waitOptions_m);
		return waitOptions;
	}

	MNamespaceCreationOptions ^ MSession::getNamespaceCreationOptions() {
		SKNamespaceCreationOptions * pNscOptions = ((SKSession *)pImpl)->getNamespaceCreationOptions();
		SKNamespaceCreationOptions_M ^ nscOptions_m = gcnew SKNamespaceCreationOptions_M;
		nscOptions_m->pNcOptions  = pNscOptions;
		MNamespaceCreationOptions ^ nscOptions = gcnew MNamespaceCreationOptions(nscOptions_m);
		return nscOptions;
	}

	MNamespaceOptions ^ MSession::getDefaultNamespaceOptions() {
		SKNamespaceOptions * pNsOptions = ((SKSession *)pImpl)->getDefaultNamespaceOptions();
		SKNamespaceOptions_M ^ nsOptions_m = gcnew SKNamespaceOptions_M;
		nsOptions_m->pNsOptions  = pNsOptions;
		MNamespaceOptions ^ nsOptions = gcnew MNamespaceOptions(nsOptions_m);
		return nsOptions;
	}

	MAsyncNSPerspective ^ MSession::openAsyncNamespacePerspective(System::String ^ ns, 
		MNamespacePerspectiveOptions ^ nspOptions) 
	{
		SKNamespacePerspectiveOptions * pNspOptions = (SKNamespacePerspectiveOptions *)(nspOptions->getPImpl()->pNspOptions);
		SKAsyncNSPerspective * pAnsPerspective = ((SKSession *)pImpl)->openAsyncNamespacePerspective(
			(char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(ns).ToPointer(),
			pNspOptions
		);
		SKAsyncNSPerspective_M ^ ansPerspective_m = gcnew SKAsyncNSPerspective_M;
		ansPerspective_m->pAnsp  = pAnsPerspective;
		MAsyncNSPerspective ^ ansPerspective = gcnew MAsyncNSPerspective(ansPerspective_m);
		return ansPerspective;
	}

	MSyncNSPerspective ^ MSession::openSyncNamespacePerspective(System::String ^ ns,
									MNamespacePerspectiveOptions ^ nspOptions)
	{
		SKNamespacePerspectiveOptions * pNspOptions = (SKNamespacePerspectiveOptions *)(nspOptions->getPImpl()->pNspOptions);
		SKSyncNSPerspective * pSnsPerspective = ((SKSession *)pImpl)->openSyncNamespacePerspective(
			(char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(ns).ToPointer(),
			pNspOptions
		);
		SKSyncNSPerspective_M ^ snsPerspective_m = gcnew SKSyncNSPerspective_M;
		snsPerspective_m->pSnsp  = pSnsPerspective;
		MSyncNSPerspective ^ snsPerspective = gcnew MSyncNSPerspective(snsPerspective_m);
		return snsPerspective;
	}

	void MSession::close() {
		((SKSession *)pImpl)->close();
	}


}