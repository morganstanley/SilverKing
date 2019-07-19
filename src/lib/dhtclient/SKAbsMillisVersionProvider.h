#ifndef SKABSMILLISVERSIONPROVIDER_H
#define SKABSMILLISVERSIONPROVIDER_H

#include "skconstants.h"
#include "skbasictypes.h"
#include "SKVersionProvider.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class AbsMillisVersionProvider;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::AbsMillisVersionProvider AbsMillisVersionProvider;
class SKAbsMillisTimeSource;

class SKAbsMillisVersionProvider : public SKVersionProvider
{
public:
    SKAPI SKAbsMillisVersionProvider(SKAbsMillisTimeSource * absMillisTimeSource);
    SKAPI virtual ~SKAbsMillisVersionProvider();

protected:
    friend class SKNamespacePerspectiveOptions;
    friend class SKBaseNSPerspective;
    SKAbsMillisVersionProvider(AbsMillisVersionProvider * pAbsMillisVersionProvider);
};

#endif // SKABSMILLISVERSIONPROVIDER_H
