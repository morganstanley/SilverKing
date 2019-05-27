/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKRETRIEVALEXCEPTION_H
#define SKRETRIEVALEXCEPTION_H

#include "skconstants.h"
#include "skcontainers.h"
#include "SKClientException.h"
#include "SKStoredValue.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking { namespace cloud { namespace dht { namespace client {
        class RetrievalException;
} } } } } } } };
typedef jace::proxy::com::ms::silverking::cloud::dht::client::RetrievalException RetrievalException;

namespace jace { namespace proxy { namespace java { namespace util { 
        class Map;
} } } };
typedef jace::proxy::java::util::Map Map;
namespace jace { namespace proxy { namespace java { namespace util { 
        class Set;
} } } };
typedef jace::proxy::java::util::Set Set;

class SKRetrievalException : public SKClientException
{
public:
    SKRetrievalException(RetrievalException * pe, const char * fileName, int lineNum);
    SKAPI ~SKRetrievalException() throw ();
    SKAPI SKStoredValue* getStoredValue(string const & key) const;
    SKAPI virtual SKOperationState::SKOperationState getOperationState(string key) const ;
    SKAPI virtual SKFailureCause::SKFailureCause getFailureCause(string key) const ;
    SKAPI virtual SKVector<string> * getFailedKeys() const ;
    SKAPI virtual string getDetailedFailureMessage() const ;

private:
    //RetrievalException * pImpl;
    Map * partialResults;
    Map * operationStates;
    Map * failureCauses;
    Set * failedKeys; 

};


#endif //SKRETRIEVALEXCEPTION_H
