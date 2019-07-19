/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKASYNCSNAPSHOT_H
#define SKASYNCSNAPSHOT_H

#include "skconstants.h"
#include "skbasictypes.h"
#include "SKAsyncOperation.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class AsyncSnapshot;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::AsyncSnapshot AsyncSnapshot;


class SKAsyncSnapshot : public SKAsyncOperation
{
public:
    SKAPI virtual ~SKAsyncSnapshot();

       SKAsyncSnapshot(AsyncSnapshot * pAsyncSnapshot);
    void * getPImpl();
protected:
    SKAsyncSnapshot();
    SKAsyncSnapshot(const SKAsyncSnapshot & );
    const SKAsyncSnapshot& operator= (const SKAsyncSnapshot & );
};

#endif  //SKASYNCSNAPSHOT_H

