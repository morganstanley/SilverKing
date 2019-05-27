package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.CreationTime;
import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.MetaData;

public class SimpleMetaData implements MetaData {
    private final int           storedLength;
    private final int           uncompressedLength;
    private final long          version;
    private final long          creationTime;
    private final ValueCreator  valueCreator;
    private final byte[]         userData;
    private final byte[]        checksum;
    private final Compression   compression;
    private final ChecksumType  checksumType;
    
    public SimpleMetaData(int storedLength, int uncompressedLength,
                          long version, long creationTime,
                          ValueCreator valueCreator, 
                          byte[] userData, 
                          byte[] checksum,
                          Compression compression,
                          ChecksumType checksumType) {
        this.storedLength = storedLength;
        this.uncompressedLength = uncompressedLength;
        this.version = version;
        this.creationTime = creationTime;
        this.valueCreator = valueCreator;
        this.userData = userData;
        this.checksum = checksum;
        this.compression = compression;
        this.checksumType = checksumType;
    }
    
    public SimpleMetaData(MetaData metaData) {
        this(metaData.getStoredLength(), metaData.getUncompressedLength(),
                metaData.getVersion(), metaData.getCreationTime().inNanos(),
                metaData.getCreator(),
                metaData.getUserData(), 
                metaData.getChecksum(), 
                metaData.getCompression(),
                metaData.getChecksumType());
    }
    
    public SimpleMetaData setValueCreator(ValueCreator _valueCreator) {
        return new SimpleMetaData(storedLength, uncompressedLength,
                version, creationTime, _valueCreator,
                userData, checksum, compression, checksumType);
    }
    
    public SimpleMetaData setStoredLength(int _storedLength) {
        return new SimpleMetaData(_storedLength, uncompressedLength,
                version, creationTime, valueCreator,
                userData, checksum, compression, checksumType);
    }

    public SimpleMetaData setUncompressedLength(int _uncompressedLength) {
        return new SimpleMetaData(storedLength, _uncompressedLength,
                version, creationTime, valueCreator,
                userData, checksum, compression, checksumType);
    }
    
    public SimpleMetaData setChecksumTypeAndChecksum(ChecksumType checksumType, byte[] checksum) {
        return new SimpleMetaData(storedLength, uncompressedLength,
                version, creationTime, valueCreator,
                userData, checksum, compression, checksumType);
    }
    
    @Override
    public int getStoredLength() {
        return storedLength;
    }

    @Override
    public int getUncompressedLength() {
        return uncompressedLength;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public CreationTime getCreationTime() {
        return new CreationTime(creationTime);
    }

    @Override
    public ValueCreator getCreator() {
        return valueCreator;
    }

    @Override
    public byte[] getUserData() {
        return userData;
    }

    @Override
    public byte[] getChecksum() {
        return checksum;
    }

    @Override
    public String toString(boolean labeled) {
        return MetaDataTextUtil.toMetaDataString(this, labeled);
    }
    
    @Override
    public String toString() {
        return toString(true);
    }

    @Override
    public Compression getCompression() {
        return compression;
    }
    
    @Override
    public ChecksumType getChecksumType() {
        return checksumType;
    }
}
