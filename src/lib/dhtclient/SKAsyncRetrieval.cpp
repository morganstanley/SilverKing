/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#include "SKAsyncRetrieval.h"
#include "SKAsyncNSPerspective.h"
#include "SKStoredValue.h"
#include "SKClientException.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;

#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/java/lang/Object.h"
using ::jace::proxy::java::lang::Object;
#include "jace/proxy/java/lang/Throwable.h"
using jace::proxy::java::lang::Throwable;

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

#include "jace/proxy/com/ms/silverking/log/Log.h"
using jace::proxy::com::ms::silverking::log::Log;

#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncRetrieval.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncRetrieval;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/StoredValue.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::StoredValue;

/* protected */
SKAsyncRetrieval::SKAsyncRetrieval(){};

/* public */
SKAsyncRetrieval::SKAsyncRetrieval(AsyncRetrieval * pAsyncRetrieval){
    pImpl = pAsyncRetrieval;
}
void * SKAsyncRetrieval::getPImpl() {
    return pImpl;
}

SKAsyncRetrieval::~SKAsyncRetrieval() { 
    if(pImpl ) {
        delete pImpl;
        pImpl = NULL;
    }
};

SKMap<string,SKStoredValue * > *  SKAsyncRetrieval::_getStoredValues(bool latest) {
	SKMap<string, SKStoredValue*> * valueMap = new SKMap<string, SKStoredValue * >();
	AsyncRetrieval * pAsync = (AsyncRetrieval*)getPImpl();
    Map storedValues ;
    try {
        storedValues = latest ? pAsync->getLatestStoredValues() : pAsync->getStoredValues();
	}  catch( Throwable &t ) {
		//throw SKClientException( &t, __FILE__, __LINE__ );
		repackException(__FILE__, __LINE__ );
    }
    if(storedValues.isNull()){
        Log::fine( "No values found" );
        return valueMap;
    }

	Set entrySet(storedValues.entrySet());
	Log::fine("SKAsyncRetrieval getStoredValues ");
	for (Iterator it(entrySet.iterator()); it.hasNext();)
	{
        Map_Entry entry = java_cast<Map_Entry>(it.next());
	    String key = java_cast<String>(entry.getKey());
        try {
		    StoredValue * value = new StoredValue(java_cast<StoredValue>(entry.getValue()));
		    SKStoredValue * sv = new  SKStoredValue(value);
            /*
		    cout << "\t\t key: " << key <<endl; 
		    cout << "\t\t StoredValue: " << sv->getValue()  << endl;
		    cout << "\t\t\t getStoredLength: " << sv->getStoredLength() << endl;
		    cout << "\t\t\t getUncompressedLength: " << sv->getUncompressedLength() << endl;
		    cout << "\t\t\t getVersion: " << sv->getVersion() << endl;
		    cout << "\t\t\t getCreationTime: " << sv->getCreationTime() << endl;
		    cout << "\t\t\t getCreatorIP: " << sv->getCreatorIP() << endl;
		    cout << "\t\t\t getCreatorID: " << sv->getCreatorID() << endl;
		    cout << "\t\t\t getUserData: " << sv->getUserData() << endl;
		    cout << "\t\t\t getStorageType: " << sv->getStorageType() << endl;
            */
    		
		    valueMap->insert(StrSVMap::value_type( key.toString(), sv ) );
		}  catch( Throwable &t ) {
			//throw SKClientException( &t, __FILE__, __LINE__ );
			repackException(__FILE__, __LINE__ );
        }
        valueMap->insert(StrSVMap::value_type(key.toString(),  (SKStoredValue*) NULL));

	}
	return valueMap;
}

SKMap<string,SKStoredValue * > *  SKAsyncRetrieval::getLatestStoredValues(void) {
    return _getStoredValues(true);
}

SKMap<string,SKStoredValue * > *  SKAsyncRetrieval::getStoredValues(void) {
    return _getStoredValues(false);
}

SKStoredValue *  SKAsyncRetrieval::getStoredValue(string& key) {
    SKStoredValue   *sv = NULL;
    
    try {
	    AsyncRetrieval *pAsync = (AsyncRetrieval*)getPImpl();
        StoredValue _sv = pAsync->getStoredValue( String(key) );
        if (_sv.isNull()) {
            sv = NULL;
        } else {
            StoredValue *storedValue = new StoredValue(java_cast<StoredValue>(_sv));
            sv = new SKStoredValue(storedValue);
        }
	}  catch( Throwable &t ) {
		//throw SKClientException( &t, __FILE__, __LINE__ );
		repackException(__FILE__, __LINE__ );
    }
    return sv;
}

/*
SKStoredValue *  SKAsyncRetrieval::getStoredValue(string& key) {
    SKStoredValue * sv = NULL;
    try {
	    AsyncRetrieval * pAsync = (AsyncRetrieval*)getPImpl();
	    StoredValue * storedValue = new StoredValue(java_cast<StoredValue>(pAsync->getStoredValue( String(key) )));
	    sv = new SKStoredValue(storedValue);
	}  catch( Throwable &t ) {
		//throw SKClientException( &t, __FILE__, __LINE__ );
		repackException(__FILE__, __LINE__ );
    }
    return sv;
}
*/


