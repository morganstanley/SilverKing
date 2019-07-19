#ifndef SKRELNANOSVERSIONPROVIDER_H
#define SKRELNANOSVERSIONPROVIDER_H

#include "skconstants.h"
#include "SKVersionProvider.h"

class SKRelNanosTimeSource;
class SKRelNanosAbsMillisTimeSource;
namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class RelNanosVersionProvider;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::RelNanosVersionProvider RelNanosVersionProvider;

class SKRelNanosVersionProvider : public SKVersionProvider
{
public:
    SKAPI virtual ~SKRelNanosVersionProvider();
    SKAPI SKRelNanosVersionProvider(SKRelNanosAbsMillisTimeSource * relNanosTimeSource);

protected:
    friend class SKNamespacePerspectiveOptions;
    friend class SKBaseNSPerspective;
    SKRelNanosVersionProvider(RelNanosVersionProvider * pRelNanosVersionProvider);
};

#endif // SKRELNANOSVERSIONPROVIDER_H
