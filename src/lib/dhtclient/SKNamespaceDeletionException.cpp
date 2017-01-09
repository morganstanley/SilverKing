#include "SKNamespaceDeletionException.h"
#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using jace::instanceof;
using namespace jace;
#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/NamespaceDeletionException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::NamespaceDeletionException;

SKNamespaceDeletionException::SKNamespaceDeletionException(NamespaceDeletionException * pNle, const char * fileName, int lineNum) 
	: SKClientException(pNle, fileName, lineNum) { 	/*pImpl = pNce;*/ }

SKNamespaceDeletionException::~SKNamespaceDeletionException()  throw () { }



