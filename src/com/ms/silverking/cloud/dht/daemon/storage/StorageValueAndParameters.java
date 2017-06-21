package com.ms.silverking.cloud.dht.daemon.storage;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.common.CorruptValueException;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.MetaDataUtil;
import com.ms.silverking.cloud.dht.common.RawRetrievalResult;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.PutOperationContainer;
import com.ms.silverking.cloud.dht.net.MessageGroupPutEntry;
import com.ms.silverking.cloud.dht.net.MessageGroupRetrievalResponseEntry;
import com.ms.silverking.log.Log;
import com.ms.silverking.text.StringUtil;

public class StorageValueAndParameters extends StorageParameters {
    private final DHTKey        key;
    private final ByteBuffer    value;
    
    private static final boolean    debug = false;
    
    public StorageValueAndParameters(DHTKey key, ByteBuffer value,
                                    long version, int uncompressedSize, int compressedSize,
                                    short ccss, byte[] checksum, 
                                    byte[] valueCreator, long creationTime) {
        super(version, uncompressedSize, compressedSize, ccss, checksum, valueCreator, creationTime);
        this.key = key;
        this.value = value;
    }
    
    public StorageValueAndParameters(MessageGroupPutEntry entry, PutOperationContainer putOperationContainer,
                                    long creationTime) {
        this(entry, entry.getValue(), putOperationContainer.getVersion(),
            entry.getUncompressedLength(),
            compressedSizeNotSet,
            putOperationContainer.getCCSS(),
            entry.getChecksum(),
            putOperationContainer.getValueCreator(), 
            creationTime);
    }
    
    public DHTKey getKey() {
        return key;
    }

    public ByteBuffer getValue() {
        return value;
    }
    
    public static StorageValueAndParameters createSVP(MessageGroupRetrievalResponseEntry entry) {
        try {
            RawRetrievalResult  rawRetrievalResult;
            
            if (entry.getValue() == null) {
                Log.warning("createSVP Couldn't find value for: ", entry);
                return null;
            } else {
                if (debug) {
                    System.out.printf("Found %s\n", entry);
                }
            }
            rawRetrievalResult = new RawRetrievalResult(RetrievalType.VALUE_AND_META_DATA);
            rawRetrievalResult.setStoredValue(entry.getValue(), true, false, null);
            StorageValueAndParameters   valueAndParameters;
            
            ByteBuffer  rawValueBuffer;
            ByteBuffer  valueBuffer;
            
            //valueBuffer = (ByteBuffer)entry.getValue().duplicate().limit(rawRetrievalResult.getStoredLength());
            rawValueBuffer = entry.getValue();
            if (debug) {
                System.out.printf("key %s buf %s storedLength %d uncompressedLength %d compressedLength %d\n", 
                    entry, rawValueBuffer, rawRetrievalResult.getStoredLength(),  
                    rawRetrievalResult.getUncompressedLength(), MetaDataUtil.getCompressedLength(rawValueBuffer, 0));
                System.out.printf("rawValueBuffer %s\n", StringUtil.byteBufferToHexString(rawValueBuffer));
            }
            
            valueBuffer = (ByteBuffer)rawValueBuffer.duplicate().position(
                    rawValueBuffer.position() + MetaDataUtil.getDataOffset(rawValueBuffer, 0));
            
            // FUTURE - consider making the nsstore allow a put that just accepts the buffer as is
            // to improve performance
            
            valueAndParameters = new StorageValueAndParameters(entry, valueBuffer, 
                    rawRetrievalResult.getVersion(), 
                    rawRetrievalResult.getUncompressedLength(), 
                    MetaDataUtil.getCompressedLength(rawValueBuffer, 0), 
                    rawRetrievalResult.getCCSS(), rawRetrievalResult.getChecksum(), 
                    rawRetrievalResult.getCreator().getBytes(), rawRetrievalResult.getCreationTimeRaw());
            return valueAndParameters;
        } catch (CorruptValueException cve) {
            Log.warning("Corrupt value in convergence: ", entry);
            return null;
        }
    }        
}
