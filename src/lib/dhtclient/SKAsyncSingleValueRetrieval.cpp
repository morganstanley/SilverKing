/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/
#include "SKAsyncSingleValueRetrieval.h"
#include "SKStoredValue.h"
#include "SKClientException.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncSingleValueRetrieval.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncSingleValueRetrieval;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/StoredValue.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::StoredValue;
#include "jace/proxy/java/lang/Throwable.h"
using jace::proxy::java::lang::Throwable;
#include "jenumutil.h"

SKAsyncSingleValueRetrieval::SKAsyncSingleValueRetrieval(AsyncSingleValueRetrieval * pAsyncSingleValueRetrieval) { //FIXME
    pImpl = pAsyncSingleValueRetrieval ;
}

SKAsyncSingleValueRetrieval::~SKAsyncSingleValueRetrieval() {
    if(pImpl) {
        //AsyncSingleValueRetrieval* pAsyncSingleValueRetrieval = (AsyncSingleValueRetrieval*)pImpl;
        delete pImpl;
        pImpl = NULL;
    }
};

void * SKAsyncSingleValueRetrieval::getPImpl(){
    return pImpl;
}

SKStoredValue *  SKAsyncSingleValueRetrieval::getStoredValue(){
    StoredValue * value = NULL;
    try {
        value =  new StoredValue(java_cast<StoredValue>(
            dynamic_cast<AsyncSingleValueRetrieval*>(pImpl)->getStoredValue( )  //FIXME: dynamic_cast
        ));
    }  catch( Throwable &t ) {
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
    return new SKStoredValue(value);
}

SKVal *  SKAsyncSingleValueRetrieval::getValue(){
    SKVal * pDhtVal = NULL; 
    try {
        AsyncSingleValueRetrieval * pAsvr = dynamic_cast<AsyncSingleValueRetrieval*>(pImpl);
        ByteArray obj = java_cast<ByteArray>(pAsvr->getValue( ));
        if( !obj.isNull() ) {
            size_t valLength = obj.length();
            if(valLength > 0) {
                pDhtVal = ::convertToDhtVal(&obj);
            }
            else {
                pDhtVal = sk_create_val();
            }
        }
    }  catch( Throwable &t ) {
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
    return pDhtVal;
}
