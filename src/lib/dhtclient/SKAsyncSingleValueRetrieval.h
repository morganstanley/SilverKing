/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKASYNCSINGLEVALUERETRIEVAL_H
#define SKASYNCSINGLEVALUERETRIEVAL_H

#include "skconstants.h"
#include "SKAsyncValueRetrieval.h"
class SKStoredValue;

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class AsyncSingleValueRetrieval;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::AsyncSingleValueRetrieval AsyncSingleValueRetrieval;


class SKAsyncSingleValueRetrieval : public SKAsyncValueRetrieval
{
public:
    SKAPI SKStoredValue *  getStoredValue();
    SKAPI SKVal *  getValue();
    SKAPI virtual ~SKAsyncSingleValueRetrieval();

    SKAsyncSingleValueRetrieval(AsyncSingleValueRetrieval * pAsyncSingleValueRetrieval);
    void * getPImpl();
 
private:
 
    SKAsyncSingleValueRetrieval(const SKAsyncSingleValueRetrieval & );
    const SKAsyncSingleValueRetrieval& operator= (const SKAsyncSingleValueRetrieval & ); 
};

#endif  //SKASYNCSINGLEVALUERETRIEVAL_H

