package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.common.CCSSUtil;


public class StorageParameters {
    private final long          version;
    private final int           uncompressedSize;
    private final int           compressedSize;
    private final short         ccss;
    private final byte[]        checksum;
    private final byte[]        valueCreator;
    private final long          creationTime;
    
    public static final int   compressedSizeNotSet = -1;  
    
    public StorageParameters(long version, 
                          int uncompressedSize,
                          int compressedSize,
                          short ccss,
                          byte[] checksum, 
                          byte[] valueCreator, 
                          long creationTime) {
        this.version = version;
        this.compressedSize = compressedSize;
        this.uncompressedSize = uncompressedSize;
        this.ccss = ccss;
        this.checksum = checksum;
        this.valueCreator = valueCreator;
        this.creationTime = creationTime;
    }
        
    public long getVersion() {
        return version;
    }

    public int getUncompressedSize() {
        return uncompressedSize;
    }
    
    public boolean compressedSizeSet() {
        return compressedSize != compressedSizeNotSet;
    }
    
    public int getCompressedSize() {
        return compressedSize;
    }
    
    public short getCCSS() {
        return ccss;
    }
    
    public byte[] getChecksum() {
        return checksum;
    }
    
    public byte[] getValueCreator() {
        return valueCreator;
    }
    
    public long getCreationTime() {
        return creationTime;
    }
    
    public ChecksumType getChecksumType() {
        return CCSSUtil.getChecksumType(ccss);
    }
    
    public StorageParameters version(long version) {
        return new StorageParameters(version, uncompressedSize, compressedSize, ccss, 
                                     checksum, valueCreator, creationTime);
    }
    
    @Override
    public String toString() {
        StringBuilder   sb;
        
        sb = new StringBuilder();
        sb.append(version);
        sb.append(':');
        sb.append(uncompressedSize);
        sb.append(':');
        sb.append(compressedSize);
        sb.append(':');
        sb.append(valueCreator);
        return sb.toString();
    }
}
