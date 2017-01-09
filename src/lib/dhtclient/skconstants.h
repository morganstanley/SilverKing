#ifndef SKCONSTANTS_H
#define SKCONSTANTS_H


#ifdef _WIN32
#ifdef DHTCLIENT_EXPORTS
#    define SKAPI __declspec(dllexport)
#else
#    define SKAPI __declspec(dllimport)
#endif
#else
#    define SKAPI
#endif


#ifdef __cplusplus
extern "C" {
#endif

const char* const SK_NULL_VALUE   = "NULL";

typedef enum SKRetrievalType_t {
	VALUE = 0, 
	META_DATA = 1, 
	VALUE_AND_META_DATA = 2, 
	EXISTENCE = 3
} SKRetrievalType;

typedef enum SKWaitMode_t {
	GET, WAIT_FOR
} SKWaitMode;

typedef enum SKTimeUnit_t {
    NANOSECONDS, MICROSECONDS, MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS
} SKTimeUnit;

namespace SKCompression {
 typedef enum SKCompression_t {
    NONE, ZIP, BZIP2, SNAPPY, LZ4
 } SKCompression;
}

namespace SKChecksumType {
 typedef enum SKChecksumType_t {
    NONE, MD5, SHA_1, MURMUR3_32, MURMUR3_128
 } SKChecksumType ;
}

namespace SKKeyDigestType {
 typedef enum SKKeyDigestType_t {
    NONE, MD5, SHA_1
 } SKKeyDigestType;
}

namespace SKOperationState {
 typedef enum SKOperationState {
    INCOMPLETE, SUCCEEDED, FAILED
 } SKOperationState;
}

namespace SKFailureCause {
 typedef enum SKFailureCause {
	ERROR, TIMEOUT, MUTATION, MULTIPLE, INVALID_VERSION, 
    SIMULTANEOUS_PUT, NO_SUCH_VALUE, NO_SUCH_NAMESPACE, CORRUPT
 } SKFailureCause ;
}

namespace SKOpResult {
 typedef enum SKOpResult {
    INCOMPLETE, SUCCEEDED, ERROR, TIMEOUT, MUTATION, NO_SUCH_VALUE, 
    SIMULTANEOUS_PUT, MULTIPLE, INVALID_VERSION, NO_SUCH_NAMESPACE, CORRUPT
 } SKOpResult;
}

namespace SKStorageType {
 typedef enum SKStorageType_t {
	RAM, FILE
 } SKStorageType;
}

typedef enum LoggingLevel_t {
	LVL_ALL, LVL_LOG, LVL_FINE, LVL_INFO, LVL_WARNING, LVL_ERROR, LVL_OFF
} LoggingLevel;

//com.ms.silverking.cloud.dht.ConsistencyProtocol
typedef enum  SKConsistency_t {
    LOOSE, TWO_PHASE_COMMIT
}SKConsistency;

typedef enum  SKVersionMode_t {
    /** Only one value may exist */
    SINGLE_VERSION,
    /** Multiple versions of a value may exist. The client must explicitly specify the version. */
    CLIENT_SPECIFIED,
    /** Versions will be selected automatically from the sequence 1, 2, 3, ... */
    SEQUENTIAL,
    /** Versions will be selected automatically using the system relative time in milliseconds */
    SYSTEM_TIME_MILLIS,
    /** Versions will be selected automatically using the system relative time in nanoseconds */
    SYSTEM_TIME_NANOS
} SKVersionMode;

namespace SKTimeoutResponse { 
 typedef enum SKTimeoutResponse_t {
    EXCEPTION, TIGNORE
 } SKTimeoutResponse;
}

namespace SKNonExistenceResponse {
 typedef enum SKNonExistenceResponse_t {
    NULL_VALUE, EXCEPTION
 } SKNonExistenceResponse;
}

typedef enum SKVersionConstraintMode_t {
    LEAST, GREATEST
} SKVersionConstraintMode;

typedef enum SKRevisionMode_t {
	NO_REVISIONS,            /** No revisions allowed */
	UNRESTRICTED_REVISIONS   /** Unrestricted revisions allowed */
} SKRevisionMode;

typedef enum SKForwardingMode_t {
    DO_NOT_FORWARD, FORWARD
} SKForwardingMode;

typedef enum SKSecondaryTargetType_t {
    NodeID, AncestorClass
} SKSecondaryTargetType;

#ifdef __cplusplus
}
#endif

#endif /* SKCONSTANTS_H */
