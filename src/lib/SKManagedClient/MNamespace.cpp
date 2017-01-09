#include "StdAfx.h"
#include "MNamespace.h"
#include "MNamespaceOptions.h"
#include "MNamespacePerspectiveOptions.h"
#include "MSyncNSPerspective.h"
#include "MAsyncNSPerspective.h"

#include <stdlib.h>
#include "SKNamespace.h"
#include "SKNamespaceOptions.h"
#include "SKNamespacePerspectiveOptions.h"
#include "SKSyncNSPerspective.h"
#include "SKAsyncNSPerspective.h"

namespace SKManagedClient {


MNamespace::!MNamespace()
{
	if(pImpl)
	{
		delete (SKNamespace*)pImpl ;
		pImpl = NULL;
	}
}

MNamespace::~MNamespace()
{
	this->!MNamespace();
}

MNamespace::MNamespace(SKNamespace_M  ^ nameSpace)
{
	pImpl = nameSpace->pNamespace;
}

SKNamespace_M ^ MNamespace::getPImpl()
{
	SKNamespace_M ^ nameSpace = gcnew SKNamespace_M;
	nameSpace->pNamespace = pImpl;
	return nameSpace;
}


MNamespacePerspectiveOptions ^ MNamespace::getDefaultNSPOptions()
{
	SKNamespacePerspectiveOptions * pNspOpt = ((SKNamespace*)pImpl)->getDefaultNSPOptions();
	SKNamespacePerspectiveOptions_M ^ nspoImp = gcnew SKNamespacePerspectiveOptions_M;
	nspoImp->pNspOptions  = pNspOpt;
	MNamespacePerspectiveOptions ^ nspo = gcnew MNamespacePerspectiveOptions(nspoImp);
	return nspo;
}

MNamespaceOptions ^ MNamespace::getOptions()
{
	SKNamespaceOptions * pNsOpt = ((SKNamespace*)pImpl)->getOptions();
	SKNamespaceOptions_M ^ nsoImp = gcnew SKNamespaceOptions_M;
	nsoImp->pNsOptions  = pNsOpt;
	MNamespaceOptions ^ nso = gcnew MNamespaceOptions(nsoImp);
	return nso;
}

MAsyncNSPerspective ^ MNamespace::openAsyncPerspective(MNamespacePerspectiveOptions ^ nspOptions)
{
	SKNamespacePerspectiveOptions * pNspOpt = (SKNamespacePerspectiveOptions*) (nspOptions->getPImpl()->pNspOptions);
	SKAsyncNSPerspective * pANsp = ((SKNamespace*)pImpl)->openAsyncPerspective(pNspOpt);
	SKAsyncNSPerspective_M ^ anspImpl = gcnew SKAsyncNSPerspective_M;
	anspImpl->pAnsp  = pANsp;
	MAsyncNSPerspective ^ ansp = gcnew MAsyncNSPerspective(anspImpl);
	return ansp;
}

MSyncNSPerspective ^ MNamespace::openSyncPerspective(MNamespacePerspectiveOptions ^ nspOptions)
{
	SKNamespacePerspectiveOptions * pNspOpt = (SKNamespacePerspectiveOptions*) (nspOptions->getPImpl()->pNspOptions);
	SKAsyncNSPerspective * pSNsp = ((SKNamespace*)pImpl)->openAsyncPerspective(pNspOpt);
	SKSyncNSPerspective_M ^ snspImpl = gcnew SKSyncNSPerspective_M;
	snspImpl->pSnsp  = pSNsp;
	MSyncNSPerspective ^ snsp = gcnew MSyncNSPerspective(snspImpl);
	return snsp;
}

String ^ MNamespace::getName()
{
	char * nsName = ((SKNamespace*)pImpl)->getName();
	String ^ name = gcnew String(nsName);
	free(nsName);
	return name;
}

MNamespace ^ MNamespace::clone(String ^ name)
{
	SKNamespace * pCloneNs = ((SKNamespace*)pImpl)->clone(
		(char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(name).ToPointer()
	);
	SKNamespace_M ^ nsImpl = gcnew SKNamespace_M;
	nsImpl->pNamespace = pCloneNs;
	MNamespace ^ ns = gcnew MNamespace(nsImpl);
	return ns;
}

MNamespace ^ MNamespace::clone(String ^ name, Int64 version)
{
	SKNamespace * pCloneNs = ((SKNamespace*)pImpl)->clone(
		(char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(name).ToPointer(),
		(int64_t) version
	);
	SKNamespace_M ^ nsImpl = gcnew SKNamespace_M;
	nsImpl->pNamespace = pCloneNs;
	MNamespace ^ ns = gcnew MNamespace(nsImpl);
	return ns;
}

void MNamespace::linkTo(String ^ target)
{
	((SKNamespace*)pImpl)->linkTo(
		(char *) System::Runtime::InteropServices::Marshal::StringToHGlobalAnsi(target).ToPointer()
	);

}


}