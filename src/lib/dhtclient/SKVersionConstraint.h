#ifndef SKVERSIONCONSTRAINT_H
#define SKVERSIONCONSTRAINT_H

#include <string.h>
using std::string;
#include <stdint.h>
#include "skconstants.h"
#include "skbasictypes.h"

class SKRetrievalOptions;
class SKGetOptions;
class SKWaitOptions;
class SKBaseNSPerspective;
class SKVersionConstraint;
namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking { namespace cloud { namespace dht { 
        class VersionConstraint;
} } } } } } };
typedef jace::proxy::com::ms::silverking::cloud::dht::VersionConstraint VersionConstraint;

class SKVersionConstraint
{
public:
    SKAPI SKVersionConstraint(int64_t minVersion, int64_t maxVersion, SKVersionConstraintMode mode, int64_t maxStorageTime);
    SKAPI SKVersionConstraint(int64_t minVersion, int64_t maxVersion, SKVersionConstraintMode mode);
    SKAPI ~SKVersionConstraint();

    SKAPI static SKVersionConstraint * exactMatch(int64_t version);
    SKAPI static SKVersionConstraint * maxAboveOrEqual(int64_t threshold);
    SKAPI static SKVersionConstraint * maxBelowOrEqual(int64_t threshold);
    SKAPI static SKVersionConstraint * minAboveOrEqual(int64_t threshold);
    
    
    SKAPI int64_t getMax();
    SKAPI int64_t getMaxCreationTime();
    SKAPI int64_t getMin();
    SKAPI SKVersionConstraintMode getMode();
    SKAPI bool matches(int64_t version);
    SKAPI bool overlaps(SKVersionConstraint * other);
    SKAPI bool equals(SKVersionConstraint * other);
    SKAPI string toString();

    SKAPI SKVersionConstraint * max(int64_t newMaxVal);
    SKAPI SKVersionConstraint * min(int64_t newMinVal);
    SKAPI SKVersionConstraint * mode(SKVersionConstraintMode mode);
    SKAPI SKVersionConstraint * maxCreationTime(int64_t maxStorageTime);

private:
    VersionConstraint * pImpl;
    
    friend class SKRetrievalOptions;
    friend class SKWaitOptions;
    friend class SKGetOptions;
    friend class SKBaseNSPerspective;

    SKVersionConstraint(VersionConstraint * pVersionProviderImpl);
    VersionConstraint * getPImpl();
};

#endif // SKVERSIONCONSTRAINT_H
