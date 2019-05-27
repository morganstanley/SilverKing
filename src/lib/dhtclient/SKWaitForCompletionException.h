/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKWAITFORCOMPLETIONEXCEPTION_H
#define SKWAITFORCOMPLETIONEXCEPTION_H

#include "SKClientException.h"
#include "SKAsyncOperation.h"
#include "skconstants.h"
#include "skcontainers.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class WaitForCompletionException;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::WaitForCompletionException WaitForCompletionException;

class SKWaitForCompletionException : public SKClientException
{
public:
    SKWaitForCompletionException(WaitForCompletionException * pe, const char * fileName, int lineNum);
    SKAPI ~SKWaitForCompletionException() throw ();    
    SKAPI SKVector<SKAsyncOperation *> * getFailedOperations();

private:
    WaitForCompletionException * pImpl;
};


#endif //SKWAITFORCOMPLETIONEXCEPTION_H
