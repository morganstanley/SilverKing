////#pragma once
#ifndef SKNAMESPACE_H
#define SKNAMESPACE_H

#include <stdint.h>  //int64_t
#include <string>
using std::string;
#include "skconstants.h"
#include "skbasictypes.h"

#include "SKSyncNSPerspective.h"
#include "SKAsyncNSPerspective.h"
#include "SKNamespaceOptions.h"
#include "SKNamespacePerspectiveOptions.h"

namespace jace { namespace proxy { namespace com { namespace ms { 
    namespace silverking {namespace cloud { namespace dht { namespace client {
        class Namespace;
} } } } } } } }
typedef jace::proxy::com::ms::silverking::cloud::dht::client::Namespace Namespace;

class SKNamespace
{
public:
    SKAPI SKNamespacePerspectiveOptions * getDefaultNSPOptions();
    SKAPI SKNamespaceOptions * getOptions();
    SKAPI SKAsyncNSPerspective * openAsyncPerspective(SKNamespacePerspectiveOptions * nspOptions);
    SKAPI SKAsyncNSPerspective * openAsyncPerspective();
    SKAPI SKSyncNSPerspective * openSyncPerspective(SKNamespacePerspectiveOptions * nspOptions);
    SKAPI SKSyncNSPerspective * openSyncPerspective();
    SKAPI char * getName();  // client should cleanup string

    SKAPI SKNamespace * clone(const string &name);   //method for Non-Versioned (SINGLE_VERSION)namespaces
    SKAPI SKNamespace * clone(const char * name);    //method for Non-Versioned (SINGLE_VERSION)namespaces

    SKAPI SKNamespace * clone(const string & name, int64_t version); //method for Versioned namespaces
    SKAPI SKNamespace * clone(const char * name, int64_t version);   //method for Versioned namespaces

    SKAPI void linkTo(const string & target);     //solely for the ifusion team
    SKAPI void linkTo(const char * target);       //solely for the ifusion team

    SKAPI ~SKNamespace();

    SKNamespace(Namespace * pNamespace); //Namespace *
    Namespace *  getPImpl();
private:
    Namespace * pImpl;
};

#endif // SKNAMESPACE_H
