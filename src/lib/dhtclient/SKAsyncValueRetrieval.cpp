/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#include "jenumutil.h"
#include "skcontainers.h"
#include "SKAsyncValueRetrieval.h"
#include "SKClientException.h"
#include "SKRetrievalException.h"

#include "SKStoredValue.h"
#include "SKPutOptions.h"
#include "SKGetOptions.h"
#include "SKRetrievalOptions.h"
#include "SKWaitOptions.h"
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
#include "jace/proxy/java/lang/Throwable.h"
using jace::proxy::java::lang::Throwable;
#include "jace/proxy/com/ms/silverking/log/Log.h"
using jace::proxy::com::ms::silverking::log::Log;

#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncValueRetrieval.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncValueRetrieval;

#include "jace/proxy/com/ms/silverking/cloud/dht/RetrievalOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::RetrievalOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/WaitOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::WaitOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/PutOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::PutOptions;
#include "jace/proxy/com/ms/silverking/cloud/dht/GetOptions.h"
using jace::proxy::com::ms::silverking::cloud::dht::GetOptions;
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



/* protected */
SKAsyncValueRetrieval::SKAsyncValueRetrieval(){};

/* public */
SKAsyncValueRetrieval::SKAsyncValueRetrieval(AsyncValueRetrieval * pAsyncValueRetrieval){
    pImpl = pAsyncValueRetrieval;
}
void * SKAsyncValueRetrieval::getPImpl() {
    return pImpl;
}

SKAsyncValueRetrieval::~SKAsyncValueRetrieval() { 
    if(pImpl ) {
        delete pImpl;
        pImpl = NULL;
    }
};


SKMap<string,SKVal*> *  SKAsyncValueRetrieval::_getValues(bool latest) {
	SKMap<string, SKVal*> * pResults = new SKMap<string, SKVal*>();
	AsyncValueRetrieval * pAsync = (AsyncValueRetrieval*)getPImpl();
	Map values ;
    try {
        values = latest ? pAsync->getLatestValues() : pAsync->getValues() ;
    }  catch( Throwable &t ) {
		throw SKClientException( &t, __FILE__, __LINE__ );
    }
    if( values.isNull()){
        Log::fine( "get returned map null" );
        return pResults;
    }

	Set entrySet(values.entrySet());
	for (Iterator it(entrySet.iterator()); it.hasNext();){
		Map_Entry entry = java_cast<Map_Entry>(it.next());
		String key = java_cast<String>(entry.getKey());
		try {
			Object obj = entry.getValue();
	        if(obj.isNull()) {
		        Log::fine( key + " null value " );
		        pResults->insert(StrValMap::value_type(key, (SKVal *) NULL));
	        }
            else {
				ByteArray barr = java_cast<ByteArray>(obj);
				/*
	            if(barr.getJavaJniClass().getInternalName() == "java/lang/String" || instanceof<String>(barr) ){
		            //Log::fine( "\t value type : String" );
		            string val = (string) java_cast<String>(barr);
                    SKVal * pval = sk_create_val();
                    sk_set_val(pval, val.size(), (void*)val.c_str());
		            pResults->insert(StrValMap::value_type(key, pval));
		            //Log::fine( val );
	            }
	            else */
				{
		            //Log::fine( "\t value type: " + obj.getJavaJniClass().getInternalName() );
                    SKVal * pDhtVal = ::convertToDhtVal(&barr);
	                pResults->insert(StrValMap::value_type(key, pDhtVal ));
	            }
            }

        } catch (RetrievalException& re){
            re.printStackTrace();
            Log::warning("Caught RetrievalException in SKAsyncNSPerspective::_getValues");
            throw SKRetrievalException( &re, __FILE__, __LINE__ );
            //continue;
        } catch(Throwable& t){
            t.printStackTrace();
            //Log::warning("Caught Throwable in SKSyncNSPerspective::_retrieve(SKVector<string> const * dhtKeys, bool isWait)");
            Log::warning( string("Key : ") + (std::string)key + " --> No value" );
	        throw SKClientException( &t, __FILE__, __LINE__ );
        }
	}
	return pResults;
}


SKMap<string,SKVal*> * SKAsyncValueRetrieval::getLatestValues(void) {
	return _getValues(true);
}


SKMap<string,SKVal*> * SKAsyncValueRetrieval::getValues(void) {
	return _getValues(false);
}

SKVal* SKAsyncValueRetrieval::getValue(string * key) {
    SKVal * pDhtVal = NULL; 
    try {
	    AsyncValueRetrieval * pAsync = (AsyncValueRetrieval*)getPImpl();
		Object obj = pAsync->getValue( String(*key) );
	    if( !obj.isNull() ) {
		    ByteArray barr = java_cast<ByteArray>(obj);
            pDhtVal = ::convertToDhtVal(&barr);
	    }
	}  catch( Throwable &t ) {
		throw SKClientException( &t, __FILE__, __LINE__ );
    }
    return pDhtVal;
}

