#include "SKNamespaceCreationException.h"
#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using jace::instanceof;
using namespace jace;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/NamespaceCreationException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::NamespaceCreationException;

SKNamespaceCreationException::SKNamespaceCreationException(NamespaceCreationException * pNce, const char * fileName, int lineNum) 
    : SKClientException(pNce, fileName, lineNum) {     /*pImpl = pNce;*/ }

SKNamespaceCreationException::~SKNamespaceCreationException()  throw () { }



