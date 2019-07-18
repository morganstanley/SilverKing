/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef SKMETADATA_H
#define SKMETADATA_H

#include <stdint.h>  //int64_t
#include <cstddef>
#include "skconstants.h"

class SKValueCreator;
struct SKVal;
namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking { namespace cloud { namespace dht { namespace client {
        class MetaData;
} } } } } } } };
typedef jace::proxy::com::ms::silverking::cloud::dht::client::MetaData MetaData;

class SKMetaData
{
public:
    /**
     * Length in bytes of the stored value. This length includes metadata.
     * @return
     */
    SKAPI int getStoredLength() const;
    /**
     * Length in bytes of the actual value stored ignoring compression. 
     * @return
     */
    SKAPI int getUncompressedLength() const;
    /**
     * Version of the value stored.
     * @return
     */
    SKAPI int64_t getVersion() const;
    /**
     * Time in milliseconds that a value was stored.
     * @return
     */
    SKAPI int64_t getCreationTime() const;
    /**
     * The ValueCreator responsible for storing the value.
     * @return
     */
    SKAPI SKValueCreator * getCreator() const;
    /**
     * lockSeconds
     */
    SKAPI int16_t getLockSeconds() const;
    /**
     * User data associated with a value.
     * @return
     */
    SKAPI SKVal * getUserData() const;
    /**
     * A string representation of this MetaData. 
     * @param labeled specifies whether or not to label each MetaData member
     * @return
     */
    SKAPI char * toString(bool labeled) const;
    /**
     * The stored checksum of this value.
     * @return
     */
    SKAPI SKVal * getChecksum() const;
    /**
     * The Compression used to stored this value.
     * @return
     */
    SKAPI SKCompression::SKCompression getCompression() const ;
    /**
     * The ChecksumType used to checksum this value.
     * @return
     */
    SKAPI SKChecksumType::SKChecksumType getChecksumType() const ;

    SKAPI virtual ~SKMetaData();

protected:
    friend class SKStoredValue;
    SKMetaData(MetaData * pMdImpl);
    SKMetaData();
    MetaData * getPImpl();

    MetaData * pImpl;
};

#endif  //SKMETADATA_H

