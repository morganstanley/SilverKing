#include "SKAsyncNSPerspective.h"
#include "skcontainers.h"
#include "SKInvalidationOptions.h"
#include "SKPutOptions.h"
#include "SKRetrievalOptions.h"
#include "SKGetOptions.h"
#include "SKRetrievalOptions.h"
#include "SKWaitOptions.h"

#include "SKAsyncRetrieval.h"
#include "SKAsyncValueRetrieval.h"
#include "SKAsyncSingleValueRetrieval.h"
#include "SKAsyncSnapshot.h"
#include "SKAsyncSyncRequest.h"
#include "SKAsyncPut.h"
#include "SKAsyncInvalidation.h"
#include "SKAsyncReadableNSPerspective.h"
#include "SKAsyncWritableNSPerspective.h"
#include "jenumutil.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using jace::instanceof;
using namespace jace;
#include "jace/JArray.h"
using jace::JArray;
#include "jace/proxy/types/JByte.h"
using jace::proxy::types::JByte;
#include "jace/proxy/types/JLong.h"
using jace::proxy::types::JLong;

#include "jace/proxy/java/util/Set.h"
using jace::proxy::java::util::Set;
#include "jace/proxy/java/util/List.h"
using jace::proxy::java::util::List;
#include "jace/proxy/java/util/Map.h"
using jace::proxy::java::util::Map;
#include "jace/proxy/java/util/HashSet.h"
using jace::proxy::java::util::HashSet;
#include "jace/proxy/java/util/HashMap.h"
using jace::proxy::java::util::HashMap;
#include "jace/proxy/java/util/Map_Entry.h"
using jace::proxy::java::util::Map_Entry;
#include "jace/proxy/java/util/Iterator.h"
using jace::proxy::java::util::Iterator;

#include "jace/proxy/java/lang/Object.h"
using jace::proxy::java::lang::Object;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/com/ms/silverking/log/Log.h"
using jace::proxy::com::ms::silverking::log::Log;
#include "jace/proxy/com/ms/silverking/cloud/dht/RetrievalOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::RetrievalOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/WaitOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::WaitOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/PutOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::PutOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/GetOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::GetOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/InvalidationOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::InvalidationOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsynchronousNamespacePerspective.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsynchronousNamespacePerspective;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncRetrieval.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncRetrieval;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncValueRetrieval.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncValueRetrieval;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncSingleValueRetrieval.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncSingleValueRetrieval;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncPut.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncPut;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncInvalidation.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncInvalidation;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncSnapshot.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncSnapshot;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncSyncRequest.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncSyncRequest;


SKAsyncNSPerspective::SKAsyncNSPerspective(AsynchronousNamespacePerspective * pAsyncNSPerspective) {
    pImpl = pAsyncNSPerspective ;
}

SKAsyncNSPerspective::~SKAsyncNSPerspective() {
    if(pImpl) {
        delete pImpl;
        pImpl = NULL;
    }
};

void * SKAsyncNSPerspective::getPImpl(){
    return pImpl;
}

void SKAsyncNSPerspective::waitForActiveOps()
{
    pImpl->waitForActiveOps();
}



SKAsyncPut * SKAsyncNSPerspective::put(StrValMap const * dhtValues){
    if( !dhtValues || dhtValues->size() == 0 )
        return NULL;
    
    Map values = java_new<HashMap>();
    StrValMap::const_iterator cit ;
    for(cit = dhtValues->begin() ; cit != dhtValues->end(); cit++ ){
        SKVal * pval = cit->second;
        values.put(String(cit->first), java_cast<Object>(::convertToByteArray(pval)) );
        //cout << "\t\tput " << cit->first << " : " << (const char *) pval->m_pVal <<endl;
    }
    AsyncPut * pAsyncPut = new AsyncPut(java_cast<AsyncPut>(pImpl->put( values )));
    return new SKAsyncPut(pAsyncPut);
}

SKAsyncPut * SKAsyncNSPerspective::put(StrValMap const * dhtValues, SKPutOptions * putOptions){
    if( !dhtValues || !putOptions || dhtValues->size() == 0 )
        return NULL;
    PutOptions * putOpt = (PutOptions*)(putOptions->getPImpl());
    if(!putOpt)
        return NULL;
    Map values = java_new<HashMap>();
    StrValMap::const_iterator cit ;
    for(cit = dhtValues->begin() ; cit != dhtValues->end(); cit++ ){
        SKVal * pval = cit->second;
        values.put(String(cit->first), java_cast<Object>(::convertToByteArray(pval)) );
        //cout << "\t\tput " << cit->first << " : " << (const char *) pval->m_pVal <<endl;
    }
    AsyncPut* pAsyncPut = new AsyncPut(java_cast<AsyncPut>(pImpl->put( values, *putOpt )));
    return new SKAsyncPut(pAsyncPut);
    
}

SKAsyncPut * SKAsyncNSPerspective::put(const char * key, const SKVal * value, SKPutOptions * putOptions){
    if ( !key || !value || !putOptions )
        return NULL;
    PutOptions * putOpt = (PutOptions*) putOptions->getPImpl();
    if(!putOpt)
        return NULL;
    ByteArray byteArray = ::convertToByteArray(value);
    AsyncPut *pAsyncPut = new AsyncPut(java_cast<AsyncPut>(pImpl->put( java_cast<Object>(String(key)), java_cast<Object>(byteArray), *putOpt )));
    return new SKAsyncPut(pAsyncPut);
}

SKAsyncPut * SKAsyncNSPerspective::put(string * key, const SKVal * value, SKPutOptions * putOptions){
    if ( !key )
        return NULL;
    return this->put(key->c_str(),  value, putOptions);
}

SKAsyncPut * SKAsyncNSPerspective::put(const char * key, const SKVal * value){
    if ( !key || !value )
        return NULL;
    ByteArray byteArray = ::convertToByteArray(value);
    AsyncPut* pAsyncPut = new AsyncPut(java_cast<AsyncPut>(pImpl->put( java_cast<Object>(String(key)), java_cast<Object>(byteArray) )));
    return new SKAsyncPut(pAsyncPut);
}

SKAsyncPut * SKAsyncNSPerspective::put(string * key, const SKVal * value){
    if ( !key )
        return NULL;
    return this->put(key->c_str(),  value);
}

//SKAsyncSnapshot * SKAsyncNSPerspective::snapshot(){
//    AsyncSnapshot* pAsyncSnapshot = new AsyncSnapshot(java_cast<AsyncSnapshot>(pImpl->snapshot()));
//    return new SKAsyncSnapshot(pAsyncSnapshot);
//}

//SKAsyncSnapshot * SKAsyncNSPerspective::snapshot(int64_t version){
//    AsyncSnapshot* pAsyncSnapshot = new AsyncSnapshot(java_cast<AsyncSnapshot>(pImpl->snapshot( JLong(version) ) ));
//    return new SKAsyncSnapshot(pAsyncSnapshot);
//}

//SKAsyncSyncRequest * SKAsyncNSPerspective::syncRequest(){
//    AsyncSyncRequest* pAsyncSyncRequest = new AsyncSyncRequest(java_cast<AsyncSyncRequest>(pImpl->syncRequest( )));
//    return new SKAsyncSyncRequest(pAsyncSyncRequest);
//}

//SKAsyncSyncRequest * SKAsyncNSPerspective::syncRequest(int64_t version){
//    AsyncSyncRequest* pAsyncSyncRequest = new AsyncSyncRequest(java_cast<AsyncSyncRequest>(pImpl->syncRequest( JLong(version) )));
//    return new SKAsyncSyncRequest(pAsyncSyncRequest);
//}


//SKMap<string, SKStoredValue*> * SKAsyncNSPerspective::retrieve(SKVector<string> * keys, SKRetrievalOptions * retrievalOptions);
//SKStoredValue * SKAsyncNSPerspective::retrieve(string key, SKRetrievalOptions * retrievalOptions);


SKAsyncValueRetrieval * SKAsyncNSPerspective::_retrieve(SKVector<string> const * dhtKeys, bool isWaitFor)
{

    size_t nKeys = dhtKeys->size();
    Set keys = java_new<HashSet>();
    for(size_t i = 0; i < nKeys; i++) 
    {
        Log::fine(string("adding key to set : ") + dhtKeys->at(i));
        keys.add(String(dhtKeys->at(i)));
    }
    AsyncValueRetrieval * pAsyncValueRetrieval = NULL;
    if(isWaitFor) {
        pAsyncValueRetrieval = new AsyncValueRetrieval (pImpl->waitFor( keys ));
    } 
    else {
        pAsyncValueRetrieval = new AsyncValueRetrieval (pImpl->get( keys )); 
    }
    return new SKAsyncValueRetrieval(pAsyncValueRetrieval);
}

SKAsyncValueRetrieval * SKAsyncNSPerspective::get(SKVector<string> const * dhtKeys){
    return _retrieve(dhtKeys, false);
}

SKAsyncValueRetrieval *  SKAsyncNSPerspective::waitFor(SKVector<string> const * dhtKeys){
    return _retrieve(dhtKeys, true);
}

SKAsyncRetrieval * SKAsyncNSPerspective::_retrieve(SKVector<string> const * dhtKeys, SKRetrievalOptions * retrOptions, bool isWaitFor){
    size_t nKeys = dhtKeys->size();
    Set keys = java_new<HashSet>();
    for(size_t i = 0; i < nKeys; i++) 
    {
        keys.add(String(dhtKeys->at(i)));
    }
    AsyncRetrieval * pAsyncRetrieval = NULL;
    if ( isWaitFor ) {
        WaitOptions * pWaitOptions =  (WaitOptions*) ((SKWaitOptions*)retrOptions)->getPImpl();
        pAsyncRetrieval = new AsyncRetrieval (((AsynchronousNamespacePerspective*)pImpl)->waitFor( keys, *pWaitOptions ));   //waitFor
    }
    else {
        GetOptions * pGetOptions =  (GetOptions*) ((SKGetOptions*)retrOptions)->getPImpl();
        pAsyncRetrieval = new AsyncRetrieval (((AsynchronousNamespacePerspective*)pImpl)->get( keys, *pGetOptions ));        //get
    }
    return new SKAsyncRetrieval(pAsyncRetrieval);
}

SKAsyncRetrieval * SKAsyncNSPerspective::get(SKVector<string> const * keys, SKGetOptions * getOptions){
    return _retrieve(keys, getOptions, false);
}

SKAsyncRetrieval * SKAsyncNSPerspective::waitFor(SKVector<string> const * keys, SKWaitOptions * waitOptions){
    return _retrieve(keys, waitOptions, true);
}

SKAsyncRetrieval * SKAsyncNSPerspective::get(const char * key, SKGetOptions * getOptions){
    if ( !key )
        return NULL;
    GetOptions * pGetOptions =  (GetOptions*) getOptions->getPImpl();
    AsyncRetrieval * pAsyncRetrieval = new AsyncRetrieval (((AsynchronousNamespacePerspective*)pImpl)->get( java_cast<Object>(java_new<String>((char *)key)), *pGetOptions ));        //get
    return new SKAsyncRetrieval(pAsyncRetrieval);
}

SKAsyncRetrieval * SKAsyncNSPerspective::get(string * key, SKGetOptions * getOptions){
    if ( !key )
        return NULL;
    return this->get(key->c_str(), getOptions);
}

SKAsyncRetrieval * SKAsyncNSPerspective::waitFor(const char * key, SKWaitOptions * waitOptions){
    if ( !key )
        return NULL;
    WaitOptions * pWaitOptions =  (WaitOptions*) waitOptions->getPImpl();
    AsyncRetrieval * pAsyncRetrieval = new AsyncRetrieval (((AsynchronousNamespacePerspective*)pImpl)->waitFor( java_cast<Object>(java_new<String>((char *)key)), *pWaitOptions ));        //get
    return new SKAsyncRetrieval(pAsyncRetrieval);
}

SKAsyncRetrieval * SKAsyncNSPerspective::waitFor(string * key, SKWaitOptions * waitOptions){
    if ( !key )
        return NULL;
    return this->waitFor(key->c_str(), waitOptions);
}

SKAsyncSingleValueRetrieval * SKAsyncNSPerspective::get(const char * key){
    if ( !key )
        return NULL;
    AsyncSingleValueRetrieval * pAsyncRetrieval = new AsyncSingleValueRetrieval (((AsynchronousNamespacePerspective*)pImpl)->get( java_cast<Object>(java_new<String>((char *)key)) ));        //get
    return new SKAsyncSingleValueRetrieval(pAsyncRetrieval);
}

SKAsyncSingleValueRetrieval * SKAsyncNSPerspective::get(string * key){
    if ( !key )
        return NULL;
    return this->get(key->c_str());
}

SKAsyncSingleValueRetrieval * SKAsyncNSPerspective::waitFor(const char * key){
    if ( !key )
        return NULL;
    AsyncSingleValueRetrieval * pAsyncRetrieval = new AsyncSingleValueRetrieval (((AsynchronousNamespacePerspective*)pImpl)->get( java_cast<Object>(java_new<String>((char *)key)) ));        //get
    return new SKAsyncSingleValueRetrieval(pAsyncRetrieval);
}

SKAsyncSingleValueRetrieval * SKAsyncNSPerspective::waitFor(string * key){
    if ( !key )
        return NULL;
    return this->waitFor(key->c_str());
}


SKAsyncInvalidation *SKAsyncNSPerspective::invalidate(SKVector<string> const *dhtKeys, SKInvalidationOptions *invalidationOptions){
    size_t nKeys = dhtKeys->size();
    Set keys = java_new<HashSet>();
    for (size_t i = 0; i < nKeys; i++) {
        //Log::fine(string("adding key to set : ") + dhtKeys->at(i));
        keys.add(String(dhtKeys->at(i)));
    }    
    InvalidationOptions *pInvalidationOptions = (InvalidationOptions*)invalidationOptions->getPImpl();
    AsyncInvalidation *pAsyncInvalidation = new AsyncInvalidation(((AsynchronousNamespacePerspective*)pImpl)->invalidate(keys, *pInvalidationOptions));
    return new SKAsyncInvalidation(pAsyncInvalidation);
}

SKAsyncInvalidation *SKAsyncNSPerspective::invalidate(SKVector<string> const *dhtKeys) {
    size_t nKeys = dhtKeys->size();
    Set keys = java_new<HashSet>();
    for (size_t i = 0; i < nKeys; i++) {
        //Log::fine(string("adding key to set : ") + dhtKeys->at(i));
        keys.add(String(dhtKeys->at(i)));
    }    
    AsyncPut *pAsyncInvalidation = new AsyncPut(((AsynchronousNamespacePerspective*)pImpl)->invalidate(keys));
    return (SKAsyncInvalidation*)new SKAsyncPut(pAsyncInvalidation);
}

SKAsyncInvalidation * SKAsyncNSPerspective::invalidate(const char *key, SKInvalidationOptions *invalidationOptions){
    if (!key) {
        return NULL;
    } else {
        InvalidationOptions *pInvalidationOptions = (InvalidationOptions*)invalidationOptions->getPImpl();
        AsyncPut *pAsyncInvalidation = new AsyncPut(((AsynchronousNamespacePerspective*)pImpl)->invalidate(java_new<String>((char *)key), *pInvalidationOptions));
        return (SKAsyncInvalidation*)new SKAsyncPut(pAsyncInvalidation);
    }
}

SKAsyncInvalidation *SKAsyncNSPerspective::invalidate(string *key, SKInvalidationOptions *invalidationOptions) {
    if (!key) {
        return NULL;
    } else {
        return this->invalidate(key->c_str(), invalidationOptions);
    }
}

SKAsyncInvalidation *SKAsyncNSPerspective::invalidate(const char *key) {
    if (!key) {
        return NULL;
    } else {
        AsyncPut *pAsyncInvalidation = new AsyncPut(((AsynchronousNamespacePerspective*)pImpl)->invalidate(java_new<String>((char *)key)) );
        return (SKAsyncInvalidation*)new SKAsyncPut(pAsyncInvalidation);
    }
}

SKAsyncInvalidation *SKAsyncNSPerspective::invalidate(string *key){
    if (!key) {
        return NULL;
    } else {
        return this->invalidate(key->c_str());
    }
}
