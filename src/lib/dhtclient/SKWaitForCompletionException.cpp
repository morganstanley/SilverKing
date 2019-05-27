#include "SKWaitForCompletionException.h"
#include "SKAsyncOperation.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using jace::instanceof;
using namespace jace;
#include "jace/proxy/java/util/List.h"
using jace::proxy::java::util::List;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncOperation.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncOperation;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/WaitForCompletionException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::WaitForCompletionException;

SKWaitForCompletionException::SKWaitForCompletionException(WaitForCompletionException * pe, const char * fileName, int lineNum) 
    : SKClientException(pe, fileName, lineNum) 
{
    pImpl = pe;
    msg       = (std::string)(pe->getFailedOperations().toString() );

} 

SKWaitForCompletionException::~SKWaitForCompletionException()  throw () { 
    delete pImpl;
}

SKVector<SKAsyncOperation *> * SKWaitForCompletionException::getFailedOperations() {

    // FIXME: move this to c-tor, as exceptions (*pImpl) are usually passed through the exception stack
    WaitForCompletionException * pe = static_cast<WaitForCompletionException *>(pImpl);
    SKVector<SKAsyncOperation *> * pAsyncOps = new SKVector<SKAsyncOperation *> ();

    List failedList( pe->getFailedOperations() );
    int  sz = failedList.size();
    for(int i =0; i<sz; i++){
        AsyncOperation * pOp = new AsyncOperation(java_cast<AsyncOperation>(failedList.get(i)));
        SKAsyncOperation * pIop = new SKAsyncOperation(pOp);
        pAsyncOps->push_back( pIop );
    }
    //for (Iterator it(failedList.iterator()); it.hasNext();) {
    //    pAsyncOps->push_back( new SKAsyncOperation(new AsyncOperation(java_cast<AsyncOperation>(it.next()))) );
    //}
    return pAsyncOps;
}


