/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKNAMEPSACERECOVEREXCEPTION_H
#define SKNAMEPSACERECOVEREXCEPTION_H

#include "skconstants.h"
#include "SKClientException.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class NamespaceRecoverException;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::NamespaceRecoverException NamespaceRecoverException;

class SKNamespaceRecoverException : public SKClientException
{
public:
    SKNamespaceRecoverException(NamespaceRecoverException * pNle, const char * fileName, int lineNum);
    SKAPI ~SKNamespaceRecoverException() throw ();    
};


#endif //SKNAMEPSACERECOVEREXCEPTION_H
