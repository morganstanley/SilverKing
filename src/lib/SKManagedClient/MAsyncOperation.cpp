#include "StdAfx.h"
#include "MAsyncKeyedOperation.h"

#include <string>
using namespace std;
#include "skconstants.h"
#include "skcontainers.h"
#include "skbasictypes.h"
#include "SKAsyncKeyedOperation.h"


namespace SKManagedClient {


//AsyncOperation
SKOperationState_M MAsyncOperation::getState()
{
    SKOperationState::SKOperationState state =  ((SKAsyncOperation*)pImpl)->getState();
    return (SKOperationState_M) state;
}

SKFailureCause_M MAsyncOperation::getFailureCause()
{
    SKFailureCause::SKFailureCause failureCause =  ((SKAsyncOperation*)pImpl)->getFailureCause();
    return (SKFailureCause_M) failureCause;
}

void MAsyncOperation::waitForCompletion()
{
    ((SKAsyncOperation*)pImpl)->waitForCompletion();
}

bool MAsyncOperation::waitForCompletion(long timeout, SKTimeUnit_M unit) 
{
    return ((SKAsyncOperation*)pImpl)->waitForCompletion(timeout, (SKTimeUnit) unit );
}
void MAsyncOperation::close()
{
    ((SKAsyncOperation*)pImpl)->close();
}


//protected
MAsyncOperation::MAsyncOperation(){ 
    pImpl = NULL; 
}

/*
SKAsyncOperation_M ^ MAsyncOperation::getPImpl(){
    SKAsyncOperation_M ^ asyncOperation = gcnew SKAsyncOperation_M;
    asyncOperation->pAsyncOperation = pImpl;
    return asyncOperation;
}
*/
MAsyncOperation::~MAsyncOperation()
{
    this->!MAsyncOperation();
}

MAsyncOperation::!MAsyncOperation()
{ 
    if(pImpl) 
    {
        delete (SKAsyncOperation*)pImpl;
        pImpl = NULL; 
    }
}


}

