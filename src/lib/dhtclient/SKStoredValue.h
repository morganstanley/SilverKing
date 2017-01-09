/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKSTOREDVALUE_H
#define SKSTOREDVALUE_H

#include "SKMetaData.h"

struct SKVal;
namespace jace { namespace proxy { namespace com { namespace ms { 
	namespace silverking {namespace cloud { namespace dht { namespace client {
		class StoredValue;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::StoredValue StoredValue;


class SKStoredValue : public SKMetaData
{
public:
	SKAPI SKMetaData * getMetaData() const ;
	SKAPI SKVal * getValue() const ;
	SKAPI SKStoredValue * next() const ;

    SKAPI virtual ~SKStoredValue();
	SKStoredValue(StoredValue * pSvImpl);
protected:
    StoredValue * getPImpl();
    SKStoredValue();
    SKStoredValue(const SKStoredValue & );
    const SKStoredValue& operator= (const SKStoredValue & );

};

#endif  //SKSTOREDVALUE_H

