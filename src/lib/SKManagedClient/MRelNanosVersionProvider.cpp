#include "StdAfx.h"
#include "MRelNanosVersionProvider.h"
#include "MVersionProvider.h"
#include "MRelNanosTimeSource.h"
#include "MRelNanosAbsMillisTimeSource.h"

#include "SKRelNanosVersionProvider.h"
#include "SKVersionProvider.h"
#include "SKRelNanosTimeSource.h"
#include "SKAbsMillisTimeSource.h"
#include "SKRelNanosAbsMillisTimeSource.h"

namespace SKManagedClient {

MRelNanosVersionProvider::~MRelNanosVersionProvider(void)
{
	this->!MRelNanosVersionProvider();
}

MRelNanosVersionProvider::!MRelNanosVersionProvider(void)
{
	if(pImpl)
	{
		delete ((SKRelNanosVersionProvider*)pImpl); 
		pImpl = NULL;
	}
}

MRelNanosVersionProvider::MRelNanosVersionProvider(MRelNanosAbsMillisTimeSource ^ relNanosTimeSource)
{
	SKRelNanosAbsMillisTimeSource* pTimeSource = (SKRelNanosAbsMillisTimeSource*)(relNanosTimeSource->getPImpl()->pTimeSource);
	SKRelNanosVersionProvider* pVersionProvider = new SKRelNanosVersionProvider(pTimeSource);
	pImpl = (void *) pVersionProvider;
}

MRelNanosVersionProvider::MRelNanosVersionProvider(SKRelNanosVersionProvider_M ^ versionProviderImpl)
{
	pImpl = versionProviderImpl->pVersionProvider;
}

/*
SKRelNanosVersionProvider_M ^ MRelNanosVersionProvider::getPImpl()
{
	SKRelNanosVersionProvider_M ^ verProvider = gcnew SKRelNanosVersionProvider_M;
	verProvider->pVersionProvider = pImpl;
	return verProvider;
}

Int64 MRelNanosVersionProvider::getVersion()
{
	Int64 ver = ((SKRelNanosVersionProvider*)pImpl)->getVersion();
	return ver;
}
*/


}
