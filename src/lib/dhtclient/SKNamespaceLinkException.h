/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKNAMEPSACELINKEXCEPTION_H
#define SKNAMEPSACELINKEXCEPTION_H

#include "skconstants.h"
#include "SKClientException.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
	namespace silverking {namespace cloud { namespace dht { namespace client {
		class NamespaceLinkException;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::NamespaceLinkException NamespaceLinkException;

class SKNamespaceLinkException : public SKClientException
{
public:
	SKNamespaceLinkException(NamespaceLinkException * pNle, const char * fileName, int lineNum);
	SKAPI ~SKNamespaceLinkException() throw ();	
};


#endif //SKNAMEPSACELINKEXCEPTION_H
