#include "SKRetrievalException.h"
#include "SKStoredValue.h"

#include <iostream>
using namespace std;

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using jace::instanceof;
using namespace jace;

#include "jace/proxy/java/util/Set.h"
using jace::proxy::java::util::Set;
#include "jace/proxy/java/util/Iterator.h"
using jace::proxy::java::util::Iterator;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/java/util/Map.h"
using jace::proxy::java::util::Map;
#include "jace/proxy/java/util/Map_Entry.h"
using jace::proxy::java::util::Map_Entry;
#include "jace/proxy/com/ms/silverking/log/Log.h"
using jace::proxy::com::ms::silverking::log::Log;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/StoredValue.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::StoredValue;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/FailureCause.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::FailureCause;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/OperationState.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::OperationState;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/RetrievalException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::RetrievalException;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/impl/RetrievalExceptionImpl.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::impl::RetrievalExceptionImpl;

SKRetrievalException::SKRetrievalException(RetrievalException * pe, const char * fileName, int lineNum)
 : SKClientException(pe, fileName, lineNum)
{
    msg = (std::string) pe->getDetailedFailureMessage();
    partialResults = new Map(java_cast<Map>(pe->partialResults()));
    operationStates = new Map(java_cast<Map>(pe->operationState()));
    failureCauses = new Map(java_cast<Map>(pe->failureCause()));
    failedKeys = new Set(java_cast<Set>(pe->failedKeys()));

    std::ostringstream str ;
    str << msg << mStack << " in " << mFileName << " : " << mLineNum <<"\n" ;
    mAll = str.str();
}

SKRetrievalException::~SKRetrievalException(void) throw ()
{
    delete partialResults;
    delete operationStates;
    delete failureCauses;
    delete failedKeys;
}

SKStoredValue * SKRetrievalException::getStoredValue(string const & key) const {
    String jkey = String(key);
    if(partialResults->get( jkey ).isNull() ){
        if(!failedKeys->isNull() && !failedKeys->contains(jkey)) {
            Log::warning( string("Key : ") + key + " --> StoredValue : Null" );
        }
    }
    StoredValue * obj = new StoredValue(java_cast<StoredValue>(partialResults->get( jkey )));

    if( obj->isNull() ) {
        Log::fine( string("Key : ") + key + " --> StoredValue : NULL" );
        delete obj;
    }
    SKStoredValue * psv = new SKStoredValue( obj ) ;
    return psv;
}

//-------------------------------------------------------------------------


SKOperationState::SKOperationState SKRetrievalException::getOperationState(string key) const {
    String jkey = java_new<String>((char *)key.c_str());
    if( ! operationStates->get( jkey ).isNull() ) {
        int os = (java_cast<OperationState>(operationStates->get( jkey ))).ordinal();
        return (SKOperationState::SKOperationState) os;
    }
    else {
        Log::warning( string("Key : ") + key + " --> OperationState : Null" );
        //throw std::exception();
        return SKOperationState::FAILED;
    }

}

SKFailureCause::SKFailureCause SKRetrievalException::getFailureCause(string key) const {
    String jkey = java_new<String>((char *)key.c_str());
    if( ! failureCauses->get( jkey ).isNull() ) {
        int fc = (java_cast<FailureCause>(failureCauses->get( jkey ))).ordinal();
        return (SKFailureCause::SKFailureCause) fc;
    }
    else {
        Log::warning( string("Key : ") + key + " --> FailureCause : Null" );
        //throw std::exception();
        return SKFailureCause::ERROR;
    }
}

SKVector<string> * SKRetrievalException::getFailedKeys() const {
    SKVector<string> * pFailedKeys = new SKVector<string>();
    for (Iterator it(failedKeys->iterator()); it.hasNext();) {
        String akey = java_cast<String>(it.next());
        pFailedKeys->push_back( (string)(akey) );
    }
    return pFailedKeys;
}

string SKRetrievalException::getDetailedFailureMessage() const {
    return msg;
    //string failureMsg =  (string)(((RetrievalException*)pImpl)->getDetailedFailureMessage());
    //return failureMsg;
}
