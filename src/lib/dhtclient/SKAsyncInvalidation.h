#ifndef SKASYNCINVALIDATION_H
#define SKASYNCINVALIDATION_H

#include "skconstants.h"
#include "skbasictypes.h"
#include "SKAsyncPut.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class AsyncInvalidation;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::AsyncInvalidation AsyncInvalidation;


class SKAsyncInvalidation : public SKAsyncPut
{
public:
    SKAPI virtual ~SKAsyncInvalidation();

    SKAsyncInvalidation(AsyncInvalidation * pAsyncInvalidation);
    void * getPImpl();
    SKAPI int64_t getStoredVersionI();
protected:
    AsyncInvalidation * pImpl;
    SKAsyncInvalidation();
    SKAsyncInvalidation(const SKAsyncInvalidation & );
    const SKAsyncInvalidation& operator= (const SKAsyncInvalidation & );

};

#endif  //SKASYNCINVALIDATION_H

