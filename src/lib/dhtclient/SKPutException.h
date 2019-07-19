/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKPUTEXCEPTION_H
#define SKPUTEXCEPTION_H

#include "skconstants.h"
#include "skcontainers.h"
#include "SKClientException.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class PutException;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::PutException PutException;

namespace jace { namespace proxy { namespace java { namespace util { 
        class Map;
} } } };
typedef jace::proxy::java::util::Map Map;
namespace jace { namespace proxy { namespace java { namespace util { 
        class Set;
} } } };
typedef jace::proxy::java::util::Set Set;


class SKPutException : public SKClientException
{
public:
    SKPutException(PutException * pe, const char * fileName, int lineNum);
    SKAPI ~SKPutException() throw ();

    SKAPI virtual SKOperationState::SKOperationState getOperationState(string key) const ;
    SKAPI virtual SKFailureCause::SKFailureCause getFailureCause(string key) const ;
    SKAPI virtual SKVector<string> * getFailedKeys() const ;
    SKAPI virtual string getDetailedFailureMessage() const ;

protected:
    //PutException * pImpl;
    Map * operationStates;
    Map * failureCauses;
    Set * failedKeys; 

};


#endif //SKPUTEXCEPTION_H
