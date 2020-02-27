#include "SKSyncNSPerspective.h"
#include "skcontainers.h"
#include "skbasictypes.h"
#include "jenumutil.h"
#include "SKStoredValue.h"
#include "SKPutOptions.h"
#include "SKGetOptions.h"
#include "SKRetrievalOptions.h"
#include "SKWaitOptions.h"
#include "SKPutException.h"
#include "SKRetrievalException.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using jace::instanceof;
using namespace jace;
#include "jace/JArray.h"
using jace::JArray;
#include "jace/proxy/JObject.h"
using jace::proxy::JObject;
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
#include "jace/proxy/java/lang/Throwable.h"
using jace::proxy::java::lang::Throwable;
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
#include "jace/proxy/com/ms/silverking/cloud/dht/client/SynchronousNamespacePerspective.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SynchronousNamespacePerspective;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/MetaData.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::MetaData;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/StoredValue.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::StoredValue;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/PutException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::PutException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/RetrievalException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::RetrievalException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/SnapshotException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SnapshotException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/SyncRequestException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SyncRequestException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/impl/PutExceptionImpl.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::impl::PutExceptionImpl;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/impl/RetrievalExceptionImpl.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::impl::RetrievalExceptionImpl;
#include <iostream>
#include <string>
#include <typeinfo>
using namespace std;

SKSyncNSPerspective::SKSyncNSPerspective(SynchronousNamespacePerspective * pSyncNSPerspective) {
    pImpl = pSyncNSPerspective ;
}

SKSyncNSPerspective::~SKSyncNSPerspective() {
    if(pImpl) {
        SynchronousNamespacePerspective* pSyncNSPerspective = (SynchronousNamespacePerspective*)pImpl;
        delete pSyncNSPerspective;
        pImpl = NULL;
    }
};

void * SKSyncNSPerspective::getPImpl(){
    return pImpl;
}


void SKSyncNSPerspective::put( SKMap<string, SKVal*> const * dhtValues){
    try {
        Map values = java_new<HashMap>();
        SKMap<string, SKVal*>::const_iterator cit ;
        for(cit = dhtValues->begin() ; cit != dhtValues->end(); cit++ ){
            SKVal * pval = cit->second;
            values.put(String(cit->first), java_cast<Object>(::convertToByteArray(pval)) );
            //cout << "\t\tput " << keys->at(i)->c_str() << " : " << (const char *) pval->m_pVal <<endl;
        }
        ((SynchronousNamespacePerspective*)pImpl)->put( values );
    } catch (PutException & pe) {
        //pe.printStackTrace();
        throw SKPutException( &pe, __FILE__, __LINE__ );
    } catch (Throwable& t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
}

void SKSyncNSPerspective::put( SKMap<string, SKVal*> const * dhtValues, SKPutOptions * pPutOptions){
    try {
        Log::fine(  "SKSyncNSPerspective put" );
        PutOptions * putOpt = (PutOptions *)pPutOptions->getPImpl();
        Map values = java_new<HashMap>();

        SKMap<string, SKVal*>::const_iterator cit ;
        for(cit = dhtValues->begin(); cit!=dhtValues->end(); cit++ ){
            SKVal * pval = cit->second;
            values.put(String(cit->first), java_cast<Object>(::convertToByteArray(pval)) );
            //cout << "\t\tput " << keys->at(i)->c_str() << " : " << (const char *) pval->m_pVal <<endl;
        }

        ((SynchronousNamespacePerspective*)pImpl)->put( values, *putOpt );
    } catch (PutException & pe) {
        throw SKPutException( &pe, __FILE__, __LINE__ );
    } catch (Throwable& t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
}

void SKSyncNSPerspective::put(const char * key, SKVal * value, SKPutOptions * pPutOptions){
    PutOptions * putOpt = (PutOptions*) pPutOptions->getPImpl();
    try {
        ((SynchronousNamespacePerspective*)pImpl)->put( java_cast<Object>(String(key)), java_cast<Object>(::convertToByteArray(value)), *putOpt );
    } catch (PutException & pe) {
        Log::fine( pe.getDetailedFailureMessage() ) ;
        //pe.printStackTrace();
        throw SKPutException( &pe, __FILE__, __LINE__ );
    } catch (Throwable& t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
}

void SKSyncNSPerspective::put(string * key, SKVal * value, SKPutOptions * pPutOptions){
    return this->put( key->c_str(), value, pPutOptions);
}

void SKSyncNSPerspective::put(const char * key, SKVal * value){
    Log::fine(  "in SKSyncNSPerspective put(string * key, SKVal * value)" );
    try {
        Object valObj = java_cast<Object>(::convertToByteArray(value));
        ((SynchronousNamespacePerspective*)pImpl)->put( String(key), valObj );
    } catch (PutException & pe) {
        Log::fine( pe.getDetailedFailureMessage() ) ;
        //pe.printStackTrace();
        throw SKPutException( &pe, __FILE__, __LINE__ );
    } catch (Throwable& t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
}

void SKSyncNSPerspective::put(string * key, SKVal * value){
    return this->put( key->c_str(), value );
}

StrValMap * SKSyncNSPerspective::_retrieve(SKVector<string> const * dhtKeys, bool isWait)
{
    StrValMap * pResults = NULL;
    size_t nKeys = dhtKeys->size();
    Set keys = java_new<HashSet>();
    for(size_t i = 0; i < nKeys; i++) 
    {
        keys.add(String(dhtKeys->at(i)));
    }

    Map values ;
    Map partResults;
    Set failedKeys;
    try {
        if(isWait) {
            values = ((SynchronousNamespacePerspective*)pImpl)->waitFor( keys ) ;
        } else {
            values = ((SynchronousNamespacePerspective*)pImpl)->get( keys );
        }
    } catch (RetrievalException& re){
        //partResults = java_cast<Map>(re.partialResults()); //TODO: add processing for this ?
        //failedKeys = java_cast<Set>(re.failedKeys());
        throw SKRetrievalException( &re, __FILE__, __LINE__ );
    } catch(Throwable& t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
    pResults = new StrValMap();
    if(values.isNull() || values.isEmpty()){
        Log::fine( "no values retireved" );
        return pResults;
    }

    Set entrySet(values.entrySet());
    if(entrySet.isNull() || entrySet.isEmpty()){
        Log::fine( "no keys retireved" );
        return pResults;
    }
    
    for (Iterator it(entrySet.iterator()); it.hasNext();){
        Map_Entry entry = java_cast<Map_Entry>(it.next());
        String key = java_cast<String>(entry.getKey());
        try {
            Object obj = entry.getValue();
            if(obj.isNull()) {
                Log::fine( key + " null value " );
                pResults->insert(StrValMap::value_type(key, (SKVal *) NULL)); // sk_create_val()
                continue;
            }
            ByteArray barr = java_cast<ByteArray>(obj);
            if(barr.isNull()) {
                pResults->insert(StrValMap::value_type(key, (SKVal *) NULL)); // sk_create_val()
                continue;
            }

            if(barr.getJavaJniClass().getInternalName() == "java/lang/String" || instanceof<String>(barr) ){
                //Log::fine( "\t value type : String" );
                string val = (string) java_cast<String>(barr);
                SKVal * pval = sk_create_val();
                sk_set_val(pval, val.size(), (void*)val.c_str());
                pResults->insert(StrValMap::value_type(key, pval));
                //Log::fine( val );
            }
            else {
                //Log::fine( "\t value type: " + obj.getJavaJniClass().getInternalName() );
                SKVal * pDhtVal = ::convertToDhtVal(&barr);
                pResults->insert(StrValMap::value_type(key, pDhtVal ));
            }

        } catch (RetrievalException& re){
            re.printStackTrace();
            Log::warning("Caught RetrievalException in SKSyncNSPerspective::_retrieve(SKVector<string> const * dhtKeys, bool isWait)");
            throw SKRetrievalException( &re, __FILE__, __LINE__ );
            //continue;
        } catch(Throwable& t){
            t.printStackTrace();
            //Log::warning("Caught Throwable in SKSyncNSPerspective::_retrieve(SKVector<string> const * dhtKeys, bool isWait)");
            Log::warning( string("Key : ") + (std::string)key + " --> No value" );
            throw SKClientException( &t, __FILE__, __LINE__ );
        }
    }
    //Log::fine( "SKSyncNSPerspective retrieve end " );
    return pResults;
}

StrValMap * SKSyncNSPerspective::get(SKVector<string> const * dhtKeys){
    return _retrieve(dhtKeys, false);
}

StrValMap *  SKSyncNSPerspective::waitFor(SKVector<string> const * dhtKeys){
    return _retrieve(dhtKeys, true);
}


StrSVMap * SKSyncNSPerspective::_retrieve(SKVector<string> const * dhtKeys, SKRetrievalOptions * retrOptions, bool isWait){
    StrSVMap * pResults = NULL;
    size_t nKeys = dhtKeys->size();
    Set keys = java_new<HashSet>();
    for(size_t i = 0; i < nKeys; i++) 
    {
        keys.add(String(dhtKeys->at(i)));
    }
    pResults = new StrSVMap();
    Map values;
    Set failedKeys; 
    try {
        if ( isWait ) {
            WaitOptions * pWaitOptions =  (WaitOptions*) ((SKWaitOptions*)retrOptions)->getPImpl();
            values = ((SynchronousNamespacePerspective*)pImpl)->waitFor( keys, *pWaitOptions );     //waitFor
        }
        else {
            GetOptions * pGetOptions =  (GetOptions*) ((SKGetOptions*)retrOptions)->getPImpl();
            values = ((SynchronousNamespacePerspective*)pImpl)->get( keys, *pGetOptions );     //get
        }
    } catch (RetrievalException& re){
        Log::logErrorWarning( re, re.getDetailedFailureMessage());
        //values = java_cast<Map>(re.partialResults());
        //failedKeys = java_cast<Set>(re.failedKeys());
        //Log::warning( failedKeys.toString());
        throw SKRetrievalException( &re, __FILE__, __LINE__ );
    } catch (Throwable& t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
    
    if(values == NULL || values.isNull()){
        Log::fine( "found no stored values" );
        return pResults;
    }

    Set entrySet(values.entrySet());
    for (Iterator it(entrySet.iterator()); it.hasNext();){
        Map_Entry entry = java_cast<Map_Entry>(it.next());
        String key = java_cast<String>(entry.getKey());
        if(entry.getValue().isNull()){
            if(!failedKeys.isNull() && !failedKeys.contains(key)) {
                Log::fine( string("Key : ") + key + " --> StoredValue : Null" );
                pResults->insert(StrSVMap::value_type((std::string)key, (SKStoredValue *) NULL )); 
            }
        }

        Object obj = entry.getValue();
        if(obj.isNull()) {
            Log::fine( string("Key : ") + (std::string)key + " --> StoredValue : NULL" );
            pResults->insert(StrSVMap::value_type((std::string)key, (SKStoredValue *)NULL ));
            continue;
        }

        StoredValue * pStoredVal = new StoredValue(java_cast<StoredValue>(obj));
        if( pStoredVal->isNull() ) {
            Log::fine( string("Key : ") + (std::string)key + " --> StoredValue : NULL" );
            pResults->insert(StrSVMap::value_type((std::string)key, (SKStoredValue *) NULL ));
            delete pStoredVal;
            continue;
        }
        SKStoredValue * isv = new SKStoredValue( pStoredVal ) ;
        pResults->insert(StrSVMap::value_type((std::string)key, isv ));
    }
    return pResults;
}

SKMap<string, SKStoredValue*> * SKSyncNSPerspective::get(SKVector<string> const * keys, SKGetOptions * getOptions){
    return _retrieve(keys, getOptions, false);
}

SKMap<string, SKStoredValue*> * SKSyncNSPerspective::waitFor(SKVector<string> const * keys, SKWaitOptions * waitOptions){
    return _retrieve(keys, waitOptions, true);
}

SKStoredValue * SKSyncNSPerspective::get(const char * key, SKGetOptions * getOptions){
    StoredValue * value = NULL;
    //GetOptions * pGetOptions =  (GetOptions*) getOptions->getPImpl();
    RetrievalOptions * pGetOptions =  (RetrievalOptions*) getOptions->getPImpl();
    String jkey  = java_new<String>((char*)key);
    try {
        Object obj = ((SynchronousNamespacePerspective*)pImpl)->retrieve( jkey, *pGetOptions );
        if(obj.isNull()) { 
            return NULL;
        }
        value =  new StoredValue(java_cast<StoredValue>(obj));

    } catch (RetrievalException& re){
        //re.printStackTrace();
        Log::logErrorWarning( re, re.toString());
        //Log::logErrorWarning( re, re.getDetailedFailureMessage());
        if( !re.partialResults().isNull() && re.partialResults().containsKey(jkey) ){
            value = new StoredValue(java_cast<StoredValue>(re.partialResults().get(jkey)));
        }
        else {
            //throw SKClientException( &re, __FILE__, __LINE__ );
            throw SKRetrievalException( &re, __FILE__, __LINE__ );
        }
    } catch (Throwable& t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
    return new SKStoredValue(value);
}

SKStoredValue * SKSyncNSPerspective::get(string * key, SKGetOptions * getOptions){
    return this->get(key->c_str(), getOptions);
}

SKStoredValue * SKSyncNSPerspective::waitFor(const char * key, SKWaitOptions * waitOptions){
    StoredValue * value = NULL;
    WaitOptions * pWaitOptions =  (WaitOptions*) waitOptions->getPImpl();
    String jkey =  java_new<String>((char*)key);
    try {
        Log::warning(waitOptions->toString());
        Object obj = ((SynchronousNamespacePerspective*)pImpl)->retrieve( jkey, *pWaitOptions );
        Log::warning(jkey);
        if(obj.isNull()) { 
            return NULL;
        }
        value =  new StoredValue(java_cast<StoredValue>(obj));
    } catch (RetrievalException& re){
        Log::logErrorWarning( re, re.getDetailedFailureMessage());
        if(!re.partialResults().isNull() && re.partialResults().containsKey(jkey)) {
            value = new StoredValue(java_cast<StoredValue>(re.partialResults().get(jkey)));
        } else {
            throw SKRetrievalException( &re, __FILE__, __LINE__ );
        }
    } catch (Throwable& t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
    if(!value->isNull()) {
        return new SKStoredValue(value);
    }
    else {
        return NULL;
    }

}

SKStoredValue * SKSyncNSPerspective::waitFor(string * key, SKWaitOptions * waitOptions){
    return this->waitFor(key->c_str(), waitOptions);
}

SKVal * SKSyncNSPerspective::get(const char * key){
    SKVal * pDhtVal = NULL;
    String jkey ;
    Object obj;
    try {
        jkey = java_new<String>((char*)key);
        obj = ((SynchronousNamespacePerspective*)pImpl)->get( jkey );
    } catch (RetrievalException& re){
        Log::logErrorWarning( re, re.toString());
        //Log::logErrorWarning( re, re.getDetailedFailureMessage());
        if(!re.partialResults().isNull() && re.partialResults().containsKey(jkey))
        {
            StoredValue value = java_cast<StoredValue>(re.partialResults().get(jkey));
            if(!value.isNull()) {
                obj = value.getValue();
            }
        }
        else {
            throw SKRetrievalException( &re, __FILE__, __LINE__ );
        }
    } catch (Throwable& t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }
    
    if(obj.isNull()) { 
        return pDhtVal;
    }
    ByteArray byteArr = java_cast<ByteArray>(obj);
    if(!byteArr.isNull()) 
        pDhtVal = ::convertToDhtVal(&byteArr);
    return pDhtVal;
}

SKVal * SKSyncNSPerspective::get(string * key){
    return this->get(key->c_str());
}

SKVal * SKSyncNSPerspective::waitFor(const char * key){
    SKVal * pDhtVal = NULL;
    String jkey ;
    Object obj;
    try {
        jkey = java_new<String>((char*)key);
        obj = ((SynchronousNamespacePerspective*)pImpl)->waitFor( jkey );
        if(obj.isNull()) { 
            return NULL;
        }
    } catch (RetrievalException& re){
        re.printStackTrace();
        Log::logErrorWarning( re, re.getDetailedFailureMessage());
        if(!re.partialResults().isNull() &&  re.partialResults().containsKey(jkey))
        {
            StoredValue value = java_cast<StoredValue>(re.partialResults().get(jkey));
            if(!value.isNull()) {
                Object obj = value.getValue();
            }
        }
        else 
            throw SKRetrievalException( &re, __FILE__, __LINE__ );
    } catch (Throwable& t){
        throw SKClientException( &t, __FILE__, __LINE__ );
    }

    if(obj.isNull()) { 
        return pDhtVal;
    }
    ByteArray byteArr = java_cast<ByteArray>(obj);
    if(!byteArr.isNull()) 
        pDhtVal = ::convertToDhtVal(&byteArr);
    return pDhtVal;
}

SKVal * SKSyncNSPerspective::waitFor(string * key){
    return this->waitFor(key->c_str());
}


