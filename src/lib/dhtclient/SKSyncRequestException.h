/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKSYNCREQUESTEXCEPTION_H
#define SKSYNCREQUESTEXCEPTION_H

#include "skconstants.h"
#include "SKClientException.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
	namespace silverking {namespace cloud { namespace dht { namespace client {
		class SyncRequestException;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::SyncRequestException SyncRequestException;

class SKSyncRequestException : public SKClientException
{
public:
	SKSyncRequestException(SyncRequestException * pSre, const char * fileName, int lineNum);
	SKAPI ~SKSyncRequestException() throw ();	
};


#endif //SKSYNCREQUESTEXCEPTION_H
