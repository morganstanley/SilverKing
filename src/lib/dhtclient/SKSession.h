#ifndef SKSESSION_H
#define SKSESSION_H

#include "skconstants.h"
#include "SKNamespace.h"

class SKSyncNSPerspective;
class SKAsyncNSPerspective;
//class SKNamespace;
class SKNamespacePerspectiveOptions;
class SKNamespaceOptions;
class SKNamespaceCreationOptions;
class SKPutOptions;
class SKGetOptions;
class SKWaitOptions;

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class DHTSession;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::DHTSession DHTSession;

class SKSession
{
public:

    SKAPI virtual ~SKSession();
    SKAPI SKNamespace * createNamespace(const char * ns, SKNamespaceOptions * nsOptions);
    SKAPI SKNamespace * createNamespace(const char * ns);
    SKAPI SKNamespace * getNamespace(const char * ns);
    SKAPI void deleteNamespace(const char * ns);  //throws SKNamespaceDeletionException
    SKAPI void recoverNamespace(const char * ns); //throws SKNamespaceRecoverException
    SKAPI SKNamespaceCreationOptions * getNamespaceCreationOptions();
    SKAPI SKNamespaceOptions * getDefaultNamespaceOptions();
    SKAPI SKPutOptions * getDefaultPutOptions();
    SKAPI SKGetOptions * getDefaultGetOptions();
    SKAPI SKWaitOptions * getDefaultWaitOptions();

    SKAPI SKAsyncNSPerspective * openAsyncNamespacePerspective(const char * ns);
    SKAPI SKAsyncNSPerspective * openAsyncNamespacePerspective(const char * ns, 
                                    SKNamespacePerspectiveOptions * pNspOptions);
    SKAPI SKSyncNSPerspective * openSyncNamespacePerspective(const char * ns);
    SKAPI SKSyncNSPerspective * openSyncNamespacePerspective(const char * ns,
                                    SKNamespacePerspectiveOptions * pNspOptions);
    SKAPI void close();
                                                                
    SKSession(DHTSession * pDHTSession);
    DHTSession * getPImpl();  //FIXME
protected:
    DHTSession * pImpl;
};

#endif // SKSESSION_H
