#include "StdAfx.h"
#include "MAsyncSnapshot.h"
#include "SKAsyncSnapshot.h"

namespace SKManagedClient {

MAsyncSnapshot::~MAsyncSnapshot(void)
{
    this->!MAsyncSnapshot();
}

MAsyncSnapshot::!MAsyncSnapshot(void)
{
    if(pImpl)
    {
        delete (SKAsyncSnapshot*)pImpl ; 
        pImpl = NULL;
    }
}

MAsyncSnapshot::MAsyncSnapshot(SKAsyncOperation_M ^ asyncSnapshot)
{
    pImpl = asyncSnapshot->pAsyncOperation;
}

SKAsyncOperation_M ^ MAsyncSnapshot::getPImpl()
{
    SKAsyncOperation_M ^ asyncSnapshot = gcnew SKAsyncOperation_M;
    asyncSnapshot->pAsyncOperation = pImpl;
    return asyncSnapshot;
}


}

