/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKASYNCVALUERETRIEVAL_H
#define SKASYNCVALUERETRIEVAL_H

#include "skconstants.h"
#include "SKAsyncRetrieval.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class AsyncValueRetrieval;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::AsyncValueRetrieval AsyncValueRetrieval;


class SKAsyncValueRetrieval : public SKAsyncRetrieval
{
public:
    SKAPI SKMap<string,SKVal*> *  getLatestValues();
    SKAPI SKMap<string,SKVal*> *  getValues() ;
    SKAPI SKVal* getValue(string * key) ;
    SKAPI virtual ~SKAsyncValueRetrieval();

    SKAsyncValueRetrieval(AsyncValueRetrieval * pAsyncValueRetrieval);
    void * getPImpl();
protected:
    SKMap<string,SKVal*> *  _getValues(bool latest) ;

    SKAsyncValueRetrieval();
    SKAsyncValueRetrieval(const SKAsyncValueRetrieval & );
    const SKAsyncValueRetrieval& operator= (const SKAsyncValueRetrieval & );

};

#endif  //SKASYNCVALUERETRIEVAL_H

