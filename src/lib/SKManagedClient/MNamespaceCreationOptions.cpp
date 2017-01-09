#include "StdAfx.h"
#include "MNamespaceCreationOptions.h"
#include "MNamespaceOptions.h"

#include <string>
using namespace std;
#include <stdlib.h>
#include "SKNamespaceCreationOptions.h"
#include "SKNamespaceOptions.h"

using namespace System::Runtime::InteropServices;

namespace SKManagedClient {

MNamespaceCreationOptions ^ MNamespaceCreationOptions::parse(String ^ def)
{
	SKNamespaceCreationOptions * pNco = SKNamespaceCreationOptions::parse(
		(char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(def).ToPointer()
	);
	SKNamespaceCreationOptions_M ^ ncOpt = gcnew SKNamespaceCreationOptions_M;
	ncOpt->pNcOptions = pNco;
	return gcnew MNamespaceCreationOptions(ncOpt);
}

MNamespaceCreationOptions ^ MNamespaceCreationOptions::defaultOptions()
{
	SKNamespaceCreationOptions * pNco = SKNamespaceCreationOptions::defaultOptions();
	SKNamespaceCreationOptions_M ^ ncOpt = gcnew SKNamespaceCreationOptions_M;
	ncOpt->pNcOptions = pNco;
	return gcnew MNamespaceCreationOptions(ncOpt);
}

MNamespaceCreationOptions::~MNamespaceCreationOptions()
{
	this->!MNamespaceCreationOptions();
}

MNamespaceCreationOptions::!MNamespaceCreationOptions()
{
	if(pImpl) 
	{
		delete (SKNamespaceCreationOptions*)pImpl ;
		pImpl = NULL;
	}
}

MNamespaceCreationOptions::MNamespaceCreationOptions(NsCreationMode_M mode, String ^ regex, MNamespaceOptions ^ defaultNSOptions)
{
		SKNamespaceOptions * pNamespaceOptions = (SKNamespaceOptions *) (defaultNSOptions->getPImpl()->pNsOptions);
		SKNamespaceCreationOptions * pNcOpt = new SKNamespaceCreationOptions( 
			(NsCreationMode) mode,
			(char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(regex).ToPointer(),
			pNamespaceOptions
		);
		pImpl = pNcOpt;
}

//impl
MNamespaceCreationOptions::MNamespaceCreationOptions(SKNamespaceCreationOptions_M ^ pNcOptions)
{
	pImpl = pNcOptions->pNcOptions;
}

SKNamespaceCreationOptions_M ^ MNamespaceCreationOptions::getPImpl()
{
	SKNamespaceCreationOptions_M ^ opt = gcnew SKNamespaceCreationOptions_M;
	opt->pNcOptions = pImpl;
	return opt;
}


bool MNamespaceCreationOptions::canBeExplicitlyCreated(String ^ ns)
{
	char* pNsName = (char*)(void*)Marshal::StringToHGlobalAnsi(ns);
	bool canBeCreated =  ((SKNamespaceCreationOptions*)pImpl)->canBeExplicitlyCreated(pNsName);
	Marshal::FreeHGlobal(System::IntPtr(pNsName));
	return canBeCreated;
}

bool MNamespaceCreationOptions::canBeAutoCreated(String ^ ns)
{
	char* pNsName = (char*)(void*)Marshal::StringToHGlobalAnsi(ns);
	bool canBeCreated =  ((SKNamespaceCreationOptions*)pImpl)->canBeAutoCreated(pNsName);
	Marshal::FreeHGlobal(System::IntPtr(pNsName));
	return canBeCreated;
}

MNamespaceOptions ^ MNamespaceCreationOptions::getDefaultNamespaceOptions()
{
	SKNamespaceOptions * pNsOpt =  ((SKNamespaceCreationOptions*)pImpl)->getDefaultNamespaceOptions();
	SKNamespaceOptions_M ^ nsOptions = gcnew SKNamespaceOptions_M;
	nsOptions->pNsOptions = pNsOpt;
	MNamespaceOptions ^ nso = gcnew MNamespaceOptions(nsOptions);
	return nso;
}

String ^ MNamespaceCreationOptions::toString()
{
	char * pStr =  ((SKNamespaceCreationOptions*)pImpl)->toString();
	String ^ str = gcnew String( pStr );
	free(pStr);
	return str;
}


}