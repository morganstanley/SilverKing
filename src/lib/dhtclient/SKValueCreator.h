#ifndef SKVALUECREATOR_H
#define SKVALUECREATOR_H

#include <cstddef>
#include "skbasictypes.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking { namespace cloud { namespace dht {
	    class ValueCreator;
} } } } } } };
typedef jace::proxy::com::ms::silverking::cloud::dht::ValueCreator ValueCreator;

/**
 * Provides information identifying the creator of values. 
 * This typically consists of the SKP address and process SKD of the creator. 
 */

class SKValueCreator
{
public:
	SKAPI static const int BYTES = 8;  //IPV4_BYTES + BYTES_PER_INT

	/** Creator IP address (IPV4) */
    SKAPI SKVal * getIP() const ;  

	/** Creator Process ID */
    SKAPI int getID() const ;
	
	/** Byte array of size BYTES with creator information : IP, ID */
    SKAPI SKVal * getBytes() const ;	

    SKAPI virtual ~SKValueCreator();
protected:
	friend class SKClient;
	friend class SKMetaData;
	friend class SKStoredValue;

    SKValueCreator(ValueCreator * pVCImpl);
	ValueCreator * getPImpl();

	ValueCreator * pImpl;
};

#endif // SKVALUECREATOR_H
