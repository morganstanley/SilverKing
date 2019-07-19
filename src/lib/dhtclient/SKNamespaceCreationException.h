/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKNAMEPSACECREATIONEXCEPTION_H
#define SKNAMEPSACECREATIONEXCEPTION_H

#include "skconstants.h"
#include "SKClientException.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class NamespaceCreationException;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::NamespaceCreationException NamespaceCreationException;

class SKNamespaceCreationException : public SKClientException
{
public:
    SKNamespaceCreationException(NamespaceCreationException * pNce, const char * fileName, int lineNum);
    SKAPI ~SKNamespaceCreationException() throw ();    
};


#endif //SKNAMEPSACECREATIONEXCEPTION_H
