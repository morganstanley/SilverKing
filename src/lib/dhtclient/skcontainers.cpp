/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#include "skcontainers.h"
#include "SKStoredValue.h"
#include "skconstants.h"
#include "skbasictypes.h"
#include "SKRetrievalException.h"
#include "SKClientException.h"
#include "jenumutil.h"

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
#include "jace/proxy/com/ms/silverking/cloud/dht/client/StoredValue.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::StoredValue;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/RetrievalException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::RetrievalException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/impl/RetrievalExceptionImpl.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::impl::RetrievalExceptionImpl;



//*********** Map *************//
template<typename T> JSKMap<T>::JSKMap( Set * pSet) : pEntrySet (pSet) { }

template<typename T> JSKMap<T>::JSKMap( Map * pMap) { 
	pEntrySet = new Set(pMap->entrySet());
	delete pMap;
}

template<typename T> JSKMap<T>::~JSKMap () {
	delete pEntrySet; pEntrySet = NULL;
}

template<typename T> unsigned int JSKMap<T>::size () const {
	return (unsigned int) pEntrySet->size();
}

template<typename T> bool JSKMap<T>::empty() const {
	return (bool) pEntrySet->isEmpty();
}

template<typename T> typename JSKMap<T>::iterator * JSKMap<T>::getIterator() {
	Iterator * pItr = new Iterator(pEntrySet->iterator());
	return new JSKMap<T>::iterator(pItr);
}

template<typename T> JSKMap<T>::iterator::iterator(Iterator * pIter) : pIterImpl(pIter) {}

template<typename T> JSKMap<T>::iterator::~iterator(){
	delete pIterImpl;
}

template<typename T> bool JSKMap<T>::iterator::hasNext() {
	return (bool) pIterImpl->hasNext();
}

//template<typename T> typename std::pair<std::string, T >* JSKMap<T>::iterator::nextVal();  //undefined

// specializations:
template<> std::pair<std::string, SKVal* >* JSKMap<SKVal*>::iterator::nextVal(){
	//static_assert(sizeof(*T) != sizeof(SKVal), "Requested Value Type is not supported!");
	std::pair<std::string, SKVal* > * pPair = NULL;
	bool hasNext = (bool) pIterImpl->hasNext();
	if(!hasNext) 
		return pPair;
	Map_Entry entry = java_cast<Map_Entry>(pIterImpl->next());
	String key = java_cast<String>(entry.getKey());
    try {
		Object obj = entry.getValue();
		if(obj.isNull()) {
		    //Log::fine( key + " null value " );
			return new std::pair<std::string, SKVal* >(key, (SKVal *) NULL); // sk_create_val()
		}
		ByteArray barr = java_cast<ByteArray>(obj);
	    if(barr.isNull()) {
		    return new std::pair<std::string, SKVal* >(key, (SKVal *) NULL); // sk_create_val()
	    }

	    if(barr.getJavaJniClass().getInternalName() == "java/lang/String" || instanceof<String>(barr) ){
		    //Log::fine( "\t value type : String" );
		    string val = (string) java_cast<String>(barr);
            SKVal * pval = sk_create_val();
            sk_set_val(pval, val.size(), (void*)val.c_str());
		    pPair = new std::pair<std::string, SKVal* >(key, pval);
		    //Log::fine( val );
	    }
	    else {
		    //Log::fine( "\t value type: " + obj.getJavaJniClass().getInternalName() );
            SKVal * pDhtVal = ::convertToDhtVal(&barr);
	        pPair = new std::pair<std::string, SKVal* >(key, pDhtVal );
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

	return pPair;
}

template<> typename std::pair<std::string, SKStoredValue* >* JSKMap<SKStoredValue*>::iterator::nextVal(){
	//static_assert(sizeof(*T) != sizeof(SKVal), "Requested Value Type is not supported!");
	std::pair<std::string, SKStoredValue* > * pPair = NULL;
	bool hasNext = (bool) pIterImpl->hasNext();
	if(!hasNext) 
		return pPair;
	Map_Entry entry = java_cast<Map_Entry>(pIterImpl->next());
	String key = java_cast<String>(entry.getKey());
    try {
		Object obj = entry.getValue();
		if(obj.isNull()) {
		    //Log::fine( key + " null value " );
			return new std::pair<std::string, SKStoredValue* >(key, (SKStoredValue *) NULL); // sk_create_val()
		}
		StoredValue * pStoredVal = new StoredValue(java_cast<StoredValue>(obj));
	    if(pStoredVal->isNull()) {
		    return new std::pair<std::string, SKStoredValue* >(key, (SKStoredValue *) NULL); // sk_create_val()
	    }

		SKStoredValue * isv = new SKStoredValue( pStoredVal ) ;
		pPair = new std::pair<std::string, SKStoredValue* >((std::string)key, isv );

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
	return pPair;
}


template class SKVector<string>;
template class SKVector<const string *>;
/*
template class SKMap<string,string>;
template class SKMap<string,SKVal*>;
template class SKMap<string,SKStoredValue *>;
template class SKMap<string,SKOperationState::SKOperationState>;
template class SKMap<string,SKFailureCause::SKFailureCause>;
*/
template class boost::unordered_map<string,string>;
template class boost::unordered_map<string,SKVal*>;
template class boost::unordered_map<string,SKStoredValue *>;
template class boost::unordered_map<string,SKOperationState::SKOperationState>;
template class boost::unordered_map<string,SKFailureCause::SKFailureCause>;

