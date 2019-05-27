#pragma once

#using <mscorlib.dll>
using namespace System;

namespace SKManagedClient {

    //  ! NB !
    // values of these enums should match those defined in the dhtclient lib
    // we will be casting enum types when cross managed/un-managed boundary
    //

    public enum struct SKRetrievalType_M  {
        VALUE = 0, 
        META_DATA = 1, 
        VALUE_AND_META_DATA = 2, 
        EXISTENCE = 3 
    };

    public enum struct SKWaitMode_M  {
        GET = 0, 
        WAIT_FOR 
    };

    public enum struct SKTimeUnit_M {
        NANOSECONDS = 0, MICROSECONDS, MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS
    };

    public enum struct SKCompression_M {
        NONE = 0, ZIP, BZIP2, SNAPPY, LZ4
    };
    
    public enum struct SKChecksumType_M {
        NONE = 0, MD5, SHA_1, MURMUR3_32, MURMUR3_128
    };
    
    public enum struct SKKeyDigestType_M {
        NONE = 0, MD5, SHA_1
    };
    
    public enum struct SKOperationState_M {
        INCOMPLETE = 0, SUCCEEDED, FAILED
    };
    
    public enum struct SKFailureCause_M {
        ERROR = 0, TIMEOUT, MUTATION, MULTIPLE, INVALID_VERSION, 
        SIMULTANEOUS_PUT, NO_SUCH_VALUE, NO_SUCH_NAMESPACE, CORRUPT
    };
    
    public enum struct SKOpResult_M {
        INCOMPLETE = 0, SUCCEEDED, ERROR, TIMEOUT, MUTATION, NO_SUCH_VALUE, 
        SIMULTANEOUS_PUT, MULTIPLE, INVALID_VERSION, NO_SUCH_NAMESPACE, CORRUPT
    };
    
    public enum struct SKStorageType_M {
        RAM = 0, FILE
    };
    
    public enum struct LoggingLevel_M {
        LVL_ALL = 0, LVL_LOG, LVL_FINE, LVL_INFO, LVL_WARNING, LVL_ERROR, LVL_OFF
    };
    
    public enum struct SKConsistency_M {
        LOOSE = 0, TWO_PHASE_COMMIT
    };
    
    public enum struct SKVersionMode_M {
        /** Only one value may exist */
        WRITE_ONCE = 0,
        /** Multiple versions of a value may exist. The client must explicitly specify the version. */
        CLIENT_SPECIFIED,
        /** Versions will be selected automatically from the sequence 1, 2, 3, ... */
        SEQUENTIAL,
        /** Versions will be selected automatically using the system relative time in milliseconds */
        SYSTEM_TIME_MILLIS,
        /** Versions will be selected automatically using the system relative time in nanoseconds */
        SYSTEM_TIME_NANOS
    };
    
    public enum struct SKTimeoutResponse_M {
        /** Throw exception on timeout */
        EXCEPTION = 0, 
        /** Ignore timeout, returns null */
        TIGNORE
    };
    
    public enum struct SKNonExistenceResponse_M {
        /** return null on key non-existance */
        NULL_VALUE = 0, 
        /** Throw exception on key non-existance */
        EXCEPTION
    };
    
    public enum struct SKVersionConstraintMode_M {
        /** choose the least version of value */
        LEAST = 0, 
        /** choose the greatest version */
        GREATEST
    };
    
    public enum struct SKRevisionMode_M {
        /** No revisions allowed */
        NO_REVISIONS = 0,
        /** Unrestricted revisions allowed */
        UNRESTRICTED_REVISIONS
    };

    public enum struct SKSecondaryTargetType_M {
        NodeID = 0, AncestorClass
    };

    // structs for passing value
    //[StructLayout(LayoutKind::Sequential, Pack=8)]
    public ref class SKVal_M
    {
    public:
      SKOperationState_M    m_rc;
      String ^                m_pVal;
    };
    
    //
    //[StructLayout(LayoutKind::Sequential, Pack=8)]
    public ref class MValMetaData
    {
    public:
      System::UInt32          uncompressLength;
      System::UInt32          storedLength;
      System::Int64           version;
      System::Int64           creationTime;
      System::UInt32          creatorIP;
      System::UInt32          creatorID;
      SKCompression_M         compression;
      SKChecksumType_M        checksumType;
      String                  ^userData;
      String                  ^value;

      virtual String ^ ToString() override;
    };

}
