#ifndef SKVERSIONPROVIDER_H
#define SKVERSIONPROVIDER_H

#include <stdint.h>
#include <cstddef>
#include "skconstants.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking { namespace cloud { namespace dht { namespace client {
	    class VersionProvider;
} } } } } } } };
typedef jace::proxy::com::ms::silverking::cloud::dht::client::VersionProvider VersionProvider;

class SKVersionProvider
{
public:
	SKAPI int64_t getVersion();
    SKAPI virtual ~SKVersionProvider();
	
protected:
	VersionProvider * pImpl;

	friend class SKNamespacePerspectiveOptions;
	friend class SKBaseNSPerspective;
    SKVersionProvider(VersionProvider * pVPImpl = NULL);
	VersionProvider * getPImpl(); 
};

#endif // SKVERSIONPROVIDER_H
