/**
*
* $Header: $
* $Change: $
* $DateTime: $
*/

#include "jenumutil.h"
#include "skbasictypes.h"
#include <iostream>
using namespace std;
#ifdef _WINDOWS
    #include <time.h>
    #include <windows.h>
#else
    #include <sys/time.h>
#endif

Compression * getCompression(SKCompression::SKCompression compression)
{
    switch(compression)
    {
        case SKCompression::NONE: 
            return new Compression (Compression::valueOf("NONE"));
        case SKCompression::ZIP: 
            return new Compression (Compression::valueOf("ZIP"));
        case SKCompression::BZIP2: 
            return new Compression (Compression::valueOf("BZIP2")); 
        case SKCompression::SNAPPY: 
            return new Compression (Compression::valueOf("SNAPPY")); 
        case SKCompression::LZ4: 
            return new Compression (Compression::valueOf("LZ4")); 
        default: 
            throw std::exception(); //FIXME:
    }
}

ChecksumType * getChecksumType(SKChecksumType::SKChecksumType checksumType)
{
    switch(checksumType)
    {
        case SKChecksumType::NONE: 
            return new ChecksumType (ChecksumType::valueOf("NONE"));
        case SKChecksumType::MD5: 
            return new ChecksumType (ChecksumType::valueOf("MD5"));
        case SKChecksumType::SHA_1: 
            return new ChecksumType (ChecksumType::valueOf("SHA_1")); 
        case SKChecksumType::MURMUR3_32: 
            return new ChecksumType (ChecksumType::valueOf("MURMUR3_32")); 
        case SKChecksumType::MURMUR3_128: 
            return new ChecksumType (ChecksumType::valueOf("MURMUR3_128")); 
        default: 
            throw std::exception(); //FIXME:
    }
}

RetrievalType * getRetrievalType(SKRetrievalType retrieveType)
{
    switch(retrieveType)
    {
        case VALUE: 
            return new RetrievalType (RetrievalType::valueOf("VALUE"));
        case META_DATA: 
            return new RetrievalType (RetrievalType::valueOf("META_DATA"));
        case VALUE_AND_META_DATA: 
            return new RetrievalType (RetrievalType::valueOf("VALUE_AND_META_DATA")); //return RetrievalType::VALUE_AND_META_DATA();
        case EXISTENCE: 
            return new RetrievalType (RetrievalType::valueOf("EXISTENCE"));
        default: 
            throw std::exception(); //FIXME:
    }
}

KeyDigestType * getDigestType(SKKeyDigestType::SKKeyDigestType keyDigestType)
{
    switch(keyDigestType)
    {
        case SKKeyDigestType::NONE: 
            return new KeyDigestType (KeyDigestType::valueOf("NONE"));
        case SKKeyDigestType::MD5: 
            return new KeyDigestType (KeyDigestType::valueOf("MD5"));
        case SKKeyDigestType::SHA_1: 
            return new KeyDigestType (KeyDigestType::valueOf("SHA_1")); 
        default: 
            throw std::exception(); //FIXME:
    }
}

WaitMode * getWaitMode(SKWaitMode waitMode)
{
    switch(waitMode)
    {
        case GET: 
            return new WaitMode (WaitMode::valueOf("GET"));
        case WAIT_FOR: 
            return new WaitMode (WaitMode::valueOf("WAIT_FOR"));
        default: 
            throw std::exception(); //FIXME:
    }
}

VersionConstraint_Mode * getVersionConstraintMode(SKVersionConstraintMode versionConstraintMode){
    switch(versionConstraintMode)
    {
        case LEAST: 
            return new VersionConstraint_Mode (VersionConstraint_Mode::valueOf("LEAST"));
        case GREATEST: 
            return new VersionConstraint_Mode (VersionConstraint_Mode::valueOf("GREATEST"));
        default: 
            throw std::exception(); //FIXME:
    }
}

NonExistenceResponse * getNonExistenceResponseType(SKNonExistenceResponse::SKNonExistenceResponse nonExistenceResponse){
    switch(nonExistenceResponse)
    {
        case SKNonExistenceResponse::NULL_VALUE: 
            return new NonExistenceResponse (NonExistenceResponse::valueOf("NULL_VALUE"));
        case SKNonExistenceResponse::EXCEPTION: 
            return new NonExistenceResponse (NonExistenceResponse::valueOf("EXCEPTION"));
        default: 
            throw std::exception(); //FIXME:
    }

}

TimeoutResponse * getTimeoutResponse(SKTimeoutResponse::SKTimeoutResponse timeoutResponse){
    switch(timeoutResponse)
    {
        case SKTimeoutResponse::EXCEPTION: 
            return new TimeoutResponse (TimeoutResponse::valueOf("EXCEPTION"));
        case SKTimeoutResponse::TIGNORE: 
            return new TimeoutResponse (TimeoutResponse::valueOf("IGNORE"));
        default: 
            throw std::exception(); //FIXME:
    }
}

StorageType * getStorageType(SKStorageType::SKStorageType storageType){
    switch(storageType)
    {
        case SKStorageType::RAM: 
            return new StorageType (StorageType::valueOf("RAM"));
        case SKStorageType::FILE: 
            return new StorageType (StorageType::valueOf("FILE"));
        default: 
            throw std::exception(); //FIXME:
    }
}

ConsistencyProtocol * getConsistencyProtocol(SKConsistency consistencyProtocol){
    switch(consistencyProtocol)
    {
        case LOOSE: 
            return new ConsistencyProtocol (ConsistencyProtocol::valueOf("LOOSE"));
        case TWO_PHASE_COMMIT: 
            return new ConsistencyProtocol (ConsistencyProtocol::valueOf("TWO_PHASE_COMMIT"));
        default: 
            throw std::exception(); //FIXME:
    }
}

NamespaceVersionMode * getVersionMode(SKVersionMode versionMode){
    switch(versionMode)
    {
        case SINGLE_VERSION: 
            return new NamespaceVersionMode (NamespaceVersionMode::valueOf("SINGLE_VERSION"));
        case CLIENT_SPECIFIED: 
            return new NamespaceVersionMode (NamespaceVersionMode::valueOf("CLIENT_SPECIFIED"));
        case SEQUENTIAL: 
            return new NamespaceVersionMode (NamespaceVersionMode::valueOf("SEQUENTIAL"));
        case SYSTEM_TIME_MILLIS: 
            return new NamespaceVersionMode (NamespaceVersionMode::valueOf("SYSTEM_TIME_MILLIS"));
        case SYSTEM_TIME_NANOS: 
            return new NamespaceVersionMode (NamespaceVersionMode::valueOf("SYSTEM_TIME_NANOS"));
        default: 
            throw std::exception(); //FIXME:
    }
}

SKVal * convertToDhtVal(ByteArray* pSrc){
    if( !pSrc )
        return NULL;
    if( pSrc->isNull() )
        return NULL;

    SKVal * pVal = sk_create_val();
    size_t valLength = pSrc->length();
    if(valLength == 0) {
        return pVal;  //empty value
    }
    JNIEnv* env = attach();
    jbyte * carr = (jbyte *) skMemAlloc(valLength, sizeof(jbyte), __FILE__, __LINE__);
    env->GetByteArrayRegion(static_cast<jbyteArray>(pSrc->getJavaJniArray()), 0, valLength, carr );
    sk_set_val_zero_copy(pVal, valLength, (void*) carr);
    return pVal;  //non-empty value
}

SKVal* convertToVal(const ByteArray * byteArray){
    if( byteArray->isNull() )
        return NULL;
    SKVal * pDhtVal = sk_create_val();
    size_t valLength = byteArray->length();
    if(valLength > 0) {
        JNIEnv* env = attach();
        jbyte * carr = (jbyte *) skMemAlloc(valLength, sizeof(jbyte), __FILE__, __LINE__);
        env->GetByteArrayRegion(static_cast<jbyteArray>(byteArray->getJavaJniArray()), 0, valLength, carr );
        sk_set_val_zero_copy(pDhtVal, valLength, (void *)carr);
        //Log::fine( string(carr, valLength) );
    }
    return pDhtVal;

}

ByteArray convertToByteArray(const SKVal * pval){
    size_t valueLen = pval->m_len;
    ByteArray byteArray(valueLen);
    if(valueLen>0) {
        JNIEnv* env = attach();
        env->SetByteArrayRegion(static_cast<jbyteArray>(byteArray.getJavaJniArray()), 0, valueLen, (const jbyte*)(pval->m_pVal));
    }
    return byteArray;
}

Level * getJavaLogLevel(LoggingLevel level){
    switch(level)
    {
        case LVL_OFF: 
            return new Level (Level::parse(java_new<String>((char*)"OFF")));
        case LVL_ERROR: 
            return new Level (Level::parse(java_new<String>((char*)"SEVERE")));
        case LVL_WARNING: 
            return new Level (Level::parse(java_new<String>((char*)"WARNING")));
        case LVL_INFO: 
            return new Level (Level::parse(java_new<String>((char*)"INFO")));
        case LVL_FINE: 
            return new Level (Level::parse(java_new<String>((char*)"FINE"))); 
        case LVL_LOG: 
            return new Level (Level::parse(java_new<String>((char*)"FINE"))); 
        case LVL_ALL: 
            return new Level (Level::parse(java_new<String>((char*)"ALL"))); 
        default: 
            return new Level (Level::parse(java_new<String>((char*)"SEVERE"))); 
    }
}

// time
uint64_t getCurTimeMs() {
    uint64_t    rVal;

    #ifdef _WINDOWS
        static const uint64_t  epoch_delta = uint64_t(116444736000000000);
         FILETIME    file_time;
         SYSTEMTIME  system_time;
         ULARGE_INTEGER ularge;
         GetSystemTime(&system_time);
         SystemTimeToFileTime(&system_time, &file_time);
         ularge.LowPart = file_time.dwLowDateTime;
         ularge.HighPart = file_time.dwHighDateTime;
         rVal = (uint64_t) ((ularge.QuadPart - epoch_delta) / 10000L) + (uint64_t) system_time.wMilliseconds;
    #else
        struct timeval    timevl;
        gettimeofday(&timevl, NULL);
        rVal = (uint64_t)timevl.tv_sec * (uint64_t)1000 + (uint64_t)timevl.tv_usec / (uint64_t)1000;
    #endif

    return rVal;
}

ForwardingMode * getForwardingMode(SKForwardingMode forwardingMode)
{
    switch(forwardingMode)
    {
        case DO_NOT_FORWARD: 
            return new ForwardingMode (ForwardingMode::valueOf("DO_NOT_FORWARD"));
        case FORWARD: 
            return new ForwardingMode (ForwardingMode::valueOf("FORWARD"));
        default: 
            throw std::exception(); //FIXME:
    }
}


