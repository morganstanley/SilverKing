#include "StdAfx.h"
#include "MAsyncPut.h"

#include "SKAsyncPut.h"


namespace SKManagedClient {


MAsyncPut::~MAsyncPut(void)
{
	this->!MAsyncPut();
}

MAsyncPut::!MAsyncPut(void)
{
	if(pImpl)
	{
		delete (SKAsyncPut*)pImpl ; 
		pImpl = NULL;
	}
}

MAsyncPut::MAsyncPut(SKAsyncOperation_M ^ asyncPut)
{
	pImpl = asyncPut->pAsyncOperation;
}

SKAsyncOperation_M ^ MAsyncPut::getPImpl()
{
	SKAsyncOperation_M ^ asyncPut = gcnew SKAsyncOperation_M;
	asyncPut->pAsyncOperation = pImpl;
	return asyncPut;
}


}

