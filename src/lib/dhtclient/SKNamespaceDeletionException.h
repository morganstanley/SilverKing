/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKNAMEPSACEDELETIONEXCEPTION_H
#define SKNAMEPSACEDELETIONEXCEPTION_H

#include "skconstants.h"
#include "SKClientException.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
	namespace silverking {namespace cloud { namespace dht { namespace client {
		class NamespaceDeletionException;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::NamespaceDeletionException NamespaceDeletionException;

class SKNamespaceDeletionException : public SKClientException
{
public:
	SKNamespaceDeletionException(NamespaceDeletionException * pNle, const char * fileName, int lineNum);
	SKAPI ~SKNamespaceDeletionException() throw ();	
};


#endif //SKNAMEPSACEDELETIONEXCEPTION_H
