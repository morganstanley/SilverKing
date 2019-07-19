/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKSNAPSHOTEXCEPTION_H
#define SKSNAPSHOTEXCEPTION_H

#include "SKClientException.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class SnapshotException;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::SnapshotException SnapshotException;

class SKSnapshotException : public SKClientException
{
public:
    SKSnapshotException(SnapshotException * pSe, const char * fileName, int lineNum);
    SKAPI ~SKSnapshotException() throw ();    
};


#endif //SKSNAPSHOTEXCEPTION_H
