package com.ms.silverking.cloud.dht.daemon.storage;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.MetaDataUtil;

public class DataSegmentWalkEntry {
    private final DHTKey        key;
    private final long          version;
    private final int           offset;
    private final int           storedLength;
    private final int           uncompressedLength;
    private final int           compressedLength;
    private final int           keyLength;
    private final ByteBuffer    storedFormat;
    private final long          creationTime;
    private final ValueCreator	creator;
    private final byte			storageState;
    
    DataSegmentWalkEntry(DHTKey key, long version, int offset, int storedLength, 
            int uncompressedLength, int compressedLength, int keyLength, ByteBuffer storedFormat,
            long creationTime, ValueCreator creator, byte storageState) {
        this.key = key;
        this.version = version;
        this.offset = offset;
        this.storedLength = storedLength;
        this.uncompressedLength = uncompressedLength;
        this.compressedLength = compressedLength;
        this.keyLength = keyLength;
        //System.out.println(storedFormat);
        this.storedFormat = storedFormat;
        this.creationTime = creationTime;
        this.creator = creator;
        this.storageState = storageState;
    }
    
    public DHTKey getKey() {
        return key;
    }
    
    public long getVersion() {
        return version;
    }
    
    public int getOffset() {
        return offset;
    }
    
    public int getStoredLength() {
        return storedLength;
    }
    
    public int getUncompressedLength() {
        return uncompressedLength;
    }
    
    public int getCompressedLength() {
        return compressedLength;
    }
    
    public int getKeyLength() {
        return keyLength;
    }
    
    public long getCreationTime() {
        return creationTime;
    }
    
    public ValueCreator getCreator() {
    	return creator;
    }
    
    public int nextEntryOffset() {
        return offset + storedLength + keyLength;
    }
    
    public byte getStorageState() {
    	return storageState;
    }
    
    public ByteBuffer getStoredFormat() {
    	return storedFormat;
    }
    
    public StorageParameters getStorageParameters() {
    	//System.out.println(storedFormat);
		//System.out.println(MetaDataUtil.getCCSS(storedFormat, 0));
		//System.out.println(MetaDataUtil.getChecksumType(storedFormat, 0));
    	return new StorageParameters(version, 
    			uncompressedLength, 
    			compressedLength,
    			MetaDataUtil.getCCSS(storedFormat, 0),
    			MetaDataUtil.getChecksum(storedFormat, 0), 
                creator.getBytes(), 
                creationTime);
    	
    }
    
    @Override
    public String toString() {
        return KeyUtil.keyToString(key) +":"+ offset +":"+ storedLength +":"+ uncompressedLength +":"+ compressedLength +":"+ keyLength 
                +":"+ version +":"+ creationTime +":"+ creator +":"+ storageState;
    }

    public byte[] getUserData() {
        return MetaDataUtil.getUserData(storedFormat, 0);
    }
}
