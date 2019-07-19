#include "SKSnapshotException.h"

#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/SnapshotException.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::SnapshotException;

SKSnapshotException::SKSnapshotException(SnapshotException * pSe, const char * fileName, int lineNum) 
        : SKClientException(pSe, fileName, lineNum) {     /*pImpl = pSe;*/ } 

SKSnapshotException::~SKSnapshotException()  throw () { }


