/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKSYNCNSPERSPECTIVE_H
#define SKSYNCNSPERSPECTIVE_H

#include "SKSyncReadableNSPerspective.h"
#include "SKSyncWritableNSPerspective.h"
#include "skconstants.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class SynchronousNamespacePerspective;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::SynchronousNamespacePerspective SynchronousNamespacePerspective;

class SKStoredValue;
class SKRetrievalOptions;

class SKSyncNSPerspective : public SKSyncReadableNSPerspective, public SKSyncWritableNSPerspective
{
public:
    SKAPI virtual void put( SKMap<string, SKVal*> const * dhtValues);
    SKAPI virtual void put( SKMap<string, SKVal*> const * dhtValues, SKPutOptions * pPutOptions);
    SKAPI virtual void put(string * key, SKVal* value, SKPutOptions * pPutOptions);
    SKAPI virtual void put(string * key, SKVal* value);
    SKAPI virtual void put(const char * key, SKVal* value, SKPutOptions * pPutOptions);
    SKAPI virtual void put(const char * key, SKVal* value);

    //virtual SKMap<string, SKStoredValue*> * retrieve(SKVector<string> * keys, SKRetrievalOptions * retrievalOptions);
    //virtual SKStoredValue * retrieve(string key, SKRetrievalOptions * retrievalOptions);
    // get - do not wait for key-value pairs to exist
    SKAPI virtual SKMap<string, SKStoredValue*> * get(SKVector<string> const * keys, SKGetOptions * getOptions);
    SKAPI virtual SKMap<string, SKVal*> * get(SKVector<string> const * keys);
    SKAPI virtual SKStoredValue * get(string * key, SKGetOptions * getOptions);
    SKAPI virtual SKVal * get(string * key);
    SKAPI virtual SKStoredValue * get(const char * key, SKGetOptions * getOptions);
    SKAPI virtual SKVal * get(const char * key);
        
    // waitFor - wait on non-existent key-value pairs
    SKAPI virtual SKMap<string, SKStoredValue*> * waitFor(SKVector<string> const * keys, SKWaitOptions * waitOptions);
    SKAPI virtual SKMap<string, SKVal*> *  waitFor(SKVector<string> const * keys);
    SKAPI virtual SKStoredValue * waitFor(string * key, SKWaitOptions * waitOptions);
    SKAPI virtual SKVal * waitFor(string * key);
    SKAPI virtual SKStoredValue * waitFor(const char * key, SKWaitOptions * waitOptions);
    SKAPI virtual SKVal * waitFor(const char * key);
    
    SKAPI virtual ~SKSyncNSPerspective();

    SKSyncNSPerspective(SynchronousNamespacePerspective * pSyncNSPerspective);
    void * getPImpl();
private:
    SynchronousNamespacePerspective * pImpl;
    
    SKMap<string, SKVal*> * _retrieve(SKVector<string> const * dhtKeys, bool isWait);
    SKMap<string, SKStoredValue*> * _retrieve(SKVector<string> const * dhtKeys, 
                            SKRetrievalOptions * retrOptions, bool isWait);
};


#endif  //SKSYNCNSPERSPECTIVE_H
