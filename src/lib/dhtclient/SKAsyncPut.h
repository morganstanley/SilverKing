/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKASYNCPUT_H
#define SKASYNCPUT_H

#include "skconstants.h"
#include "skbasictypes.h"
#include "SKAsyncKeyedOperation.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class AsyncPut;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::AsyncPut AsyncPut;


class SKAsyncPut: public SKAsyncKeyedOperation
{
public:
    SKAPI virtual ~SKAsyncPut();

       SKAsyncPut(AsyncPut * pAsyncPut);
    void * getPImpl();
protected:
    SKAsyncPut();
    SKAsyncPut(const SKAsyncPut & );
    const SKAsyncPut& operator= (const SKAsyncPut & );

};

#endif  //SKASYNCPUT_H

