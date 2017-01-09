#include "StdAfx.h"
#include "MAsyncSyncRequest.h"
#include "SKAsyncSyncRequest.h"

namespace SKManagedClient {

MAsyncSyncRequest::~MAsyncSyncRequest(void)
{
	this->!MAsyncSyncRequest();
}

MAsyncSyncRequest::!MAsyncSyncRequest(void)
{
	if(pImpl)
	{
		delete (SKAsyncSyncRequest*)pImpl ; 
		pImpl = NULL;
	}
}

MAsyncSyncRequest::MAsyncSyncRequest(SKAsyncOperation_M ^ asyncSyncRequest)
{
	pImpl = asyncSyncRequest->pAsyncOperation;
}

SKAsyncOperation_M ^ MAsyncSyncRequest::getPImpl()
{
	SKAsyncOperation_M ^ asyncSyncRequest = gcnew SKAsyncOperation_M;
	asyncSyncRequest->pAsyncOperation = pImpl;
	return asyncSyncRequest;
}


}

