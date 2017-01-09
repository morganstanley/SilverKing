#include "StdAfx.h"
#include "MVersionProvider.h"

#include <stdlib.h>
#include <string>
using namespace std;
#include "SKVersionProvider.h"

namespace SKManagedClient {

MVersionProvider::~MVersionProvider()
{
	this->!MVersionProvider();
}

MVersionProvider::!MVersionProvider()
{
	if(pImpl)
	{
		delete (SKVersionProvider*)pImpl;
		pImpl = NULL;
	}
}

SKVersionProvider_M ^ MVersionProvider::getPImpl()
{
	SKVersionProvider_M ^ vp =  gcnew SKVersionProvider_M;
	vp->pVersionProvider = pImpl;
	return vp;
}

MVersionProvider::MVersionProvider(SKVersionProvider_M ^ verProvider)
{
	pImpl = verProvider->pVersionProvider;
}

MVersionProvider::MVersionProvider() : pImpl(NULL) { }

Int64 MVersionProvider::getVersion()
{
	return ((SKVersionProvider*)pImpl)->getVersion();
}

}

