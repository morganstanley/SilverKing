/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#ifndef JENUMUTIL_H
#define JENUMUTIL_H

/////////////
// includes
#include "skconstants.h"
#include "skbasictypes.h"

#include "jace/Jace.h"
using jace::java_new;
using jace::java_cast;
using namespace jace;
#include "jace/JArray.h"
using jace::JArray;
#include "jace/proxy/types/JByte.h"
using jace::proxy::types::JByte;

#include "jace/proxy/java/lang/String.h"
using jace::proxy::java::lang::String;
#include "jace/proxy/java/util/logging/Level.h"
using jace::proxy::java::util::logging::Level;
#include "jace/proxy/com/ms/silverking/log/Log.h"
using jace::proxy::com::ms::silverking::log::Log;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/Compression.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::Compression;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/ChecksumType.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::ChecksumType;
#include "jace/proxy/com/ms/silverking/cloud/dht/ConsistencyProtocol.h"
using jace::proxy::com::ms::silverking::cloud::dht::ConsistencyProtocol;
#include "jace/proxy/com/ms/silverking/cloud/dht/client/KeyDigestType.h"
using jace::proxy::com::ms::silverking::cloud::dht::client::KeyDigestType;
#include "jace/proxy/com/ms/silverking/cloud/dht/NamespaceVersionMode.h"
using jace::proxy::com::ms::silverking::cloud::dht::NamespaceVersionMode;
#include "jace/proxy/com/ms/silverking/cloud/dht/NonExistenceResponse.h"
using jace::proxy::com::ms::silverking::cloud::dht::NonExistenceResponse;
#include "jace/proxy/com/ms/silverking/cloud/dht/RetrievalType.h"
using jace::proxy::com::ms::silverking::cloud::dht::RetrievalType;
#include "jace/proxy/com/ms/silverking/cloud/dht/RevisionMode.h"
using jace::proxy::com::ms::silverking::cloud::dht::RevisionMode;
#include "jace/proxy/com/ms/silverking/cloud/dht/StorageType.h"
using jace::proxy::com::ms::silverking::cloud::dht::StorageType;
#include "jace/proxy/com/ms/silverking/cloud/dht/TimeoutResponse.h"
using jace::proxy::com::ms::silverking::cloud::dht::TimeoutResponse;
#include "jace/proxy/com/ms/silverking/cloud/dht/VersionConstraint_Mode.h"
using jace::proxy::com::ms::silverking::cloud::dht::VersionConstraint_Mode;
#include "jace/proxy/com/ms/silverking/cloud/dht/WaitMode.h"
using jace::proxy::com::ms::silverking::cloud::dht::WaitMode;
#include "jace/proxy/com/ms/silverking/cloud/dht/net/ForwardingMode.h"
using jace::proxy::com::ms::silverking::cloud::dht::net::ForwardingMode;

typedef JArray< jace::proxy::types::JByte > ByteArray;


Compression * getCompression(SKCompression::SKCompression compression);
ChecksumType * getChecksumType(SKChecksumType::SKChecksumType checksumType);
RetrievalType * getRetrievalType(SKRetrievalType retrieveType);
KeyDigestType * getDigestType(SKKeyDigestType::SKKeyDigestType keyDigestType);
WaitMode * getWaitMode(SKWaitMode waitMode);
VersionConstraint_Mode * getVersionConstraintMode(SKVersionConstraintMode versionConstraintMode);
NonExistenceResponse * getNonExistenceResponseType(SKNonExistenceResponse::SKNonExistenceResponse nonExistenceResponse);
TimeoutResponse * getTimeoutResponse(SKTimeoutResponse::SKTimeoutResponse timeoutResponse);
StorageType * getStorageType(SKStorageType::SKStorageType storageType);
ConsistencyProtocol * getConsistencyProtocol(SKConsistency consistencyProtocol);
NamespaceVersionMode * getVersionMode(SKVersionMode versionMode);
SKVal * convertToDhtVal(ByteArray* pSrc);
ByteArray convertToByteArray(const SKVal * pval);
Level * getJavaLogLevel(LoggingLevel level);
uint64_t getCurTimeMs();
ForwardingMode * getForwardingMode(SKForwardingMode forwardingMode);
 
#endif   //JENUMUTIL_H
