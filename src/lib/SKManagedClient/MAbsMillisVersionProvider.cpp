#include "StdAfx.h"
#include "MAbsMillisVersionProvider.h"
#include "MVersionProvider.h"
#include "MAbsMillisTimeSource.h"

#include "SKAbsMillisVersionProvider.h"
#include "SKVersionProvider.h"
#include "SKAbsMillisTimeSource.h"

namespace SKManagedClient {

MAbsMillisVersionProvider::~MAbsMillisVersionProvider()
{
	this->!MAbsMillisVersionProvider();
}

MAbsMillisVersionProvider::!MAbsMillisVersionProvider()
{
	delete ((SKAbsMillisVersionProvider*)pImpl); 
	pImpl = NULL;
}

MAbsMillisVersionProvider::MAbsMillisVersionProvider(MAbsMillisTimeSource ^ absMillisTimeSource)
{
	SKAbsMillisTimeSource* pTimeSource = (SKAbsMillisTimeSource*)(absMillisTimeSource->getPImpl()->pTimeSource);
	SKAbsMillisVersionProvider* pVersionProvider = new SKAbsMillisVersionProvider(pTimeSource);
	pImpl = (void *) pVersionProvider;
}

MAbsMillisVersionProvider::MAbsMillisVersionProvider(SKAbsMillisVersionProvider_M ^ versionProviderImpl)
{
	pImpl = versionProviderImpl->pVersionProvider;
}

}