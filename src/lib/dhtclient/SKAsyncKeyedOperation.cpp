/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#include "SKAsyncKeyedOperation.h"
#include <iostream>
using std::endl;

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/JArray.h"
using jace::JArray;

#include "jace/proxy/java/lang/Object.h"
using ::jace::proxy::java::lang::Object;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;

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
#include "jace/proxy/com/ms/silverking/cloud/dht/client/AsyncKeyedOperation.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::AsyncKeyedOperation;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/OperationState.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::OperationState;

/* public */
SKAsyncKeyedOperation::SKAsyncKeyedOperation(AsyncKeyedOperation * pAsyncKeyedOperation){
    pImpl = pAsyncKeyedOperation;
}
void * SKAsyncKeyedOperation::getPImpl() {
    return pImpl;
}

SKAsyncKeyedOperation::~SKAsyncKeyedOperation() { 
    if(pImpl) {
        delete pImpl;
        pImpl = NULL;
    }
}

/*protected*/
SKAsyncKeyedOperation::SKAsyncKeyedOperation() {};


/* public methods from Java */

SKVector<string> * SKAsyncKeyedOperation::getKeys(void) {
	AsyncKeyedOperation * pAsync = (AsyncKeyedOperation*)getPImpl();
	Set entrySet = java_cast<Set>(pAsync->getKeys());
	SKVector<string> * keys = new SKVector<string>();
	typedef JArray<Object> ObjArray;
	ObjArray strs = entrySet.toArray();
    for (ObjArray::Iterator it = strs.begin(); it != strs.end(); ++it)
	{
		String str = java_cast<String>(*it);
		std::string key = str.toString();
		keys->push_back(key);
    }
	
	return keys;
}

SKOperationState::SKOperationState SKAsyncKeyedOperation::getOperationState(const string & key) {
	AsyncKeyedOperation * pAsync = (AsyncKeyedOperation*)getPImpl();
	int i = (jint) pAsync->getOperationState(String(key)).ordinal();
	return (SKOperationState::SKOperationState) i;
}

SKMap<string,SKOperationState::SKOperationState> * SKAsyncKeyedOperation::getOperationStateMap(void) {
	OpStateMap * stateMap = new OpStateMap();
	AsyncKeyedOperation * pAsync = (AsyncKeyedOperation*)getPImpl();
	Map opStateMap = java_cast<Map>(pAsync->getOperationStateMap());
	Set entrySet(opStateMap.entrySet());
	for (Iterator it(entrySet.iterator()); it.hasNext();)
	{
		Map_Entry entry = java_cast<Map_Entry>(it.next());
		String key = java_cast<String>(entry.getKey());
		OperationState value = java_cast<OperationState>(entry.getValue());
		SKOperationState::SKOperationState state = (SKOperationState::SKOperationState) ((jint) value.ordinal());
		//cout << "key: <" << key << "> state: <" << state << ">" << endl;
		stateMap->insert(OpStateMap::value_type( (std::string)key, state) );
	}
	
	return stateMap;
}

SKVector<string> * SKAsyncKeyedOperation::getIncompleteKeys() {
	AsyncKeyedOperation * pAsync = (AsyncKeyedOperation*)getPImpl();
	Set entrySet = java_cast<Set>(pAsync->getIncompleteKeys());
	SKVector<string> * keys = new SKVector<string>();
	typedef JArray<Object> ObjArray;
	ObjArray strs = entrySet.toArray();
    for (ObjArray::Iterator it = strs.begin(); it != strs.end(); ++it)
	{
		String str = java_cast<String>(*it);
		std::string key = str.toString();
		keys->push_back(key);
    }
	
	return keys;
}

int SKAsyncKeyedOperation::getNumKeys()
{
	AsyncKeyedOperation * pAsync = (AsyncKeyedOperation*)getPImpl();
	return (int) pAsync->getNumKeys();
}
