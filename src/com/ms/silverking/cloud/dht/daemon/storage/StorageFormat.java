package com.ms.silverking.cloud.dht.daemon.storage;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.common.CCSSUtil;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.MetaDataUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.text.StringUtil;

public class StorageFormat {
    private static final boolean    debug = false;
    
    static final int    writeFailedOffset = -1;
    
    public static int writeFormattedValueToBuf(DHTKey key, ByteBuffer formattedBuf, ByteBuffer buf, AtomicInteger nextFree, int writeLimit) {
    	int	writeSize;
    	int	storedLength;
    	int	offset;
    	
    	storedLength = formattedBuf.remaining();
        writeSize = storedLength + DHTKey.BYTES_PER_KEY;
        //System.out.println("writeSize: "+ writeSize);
        offset = nextFree.getAndAdd(writeSize);
        Log.fine("offset: ", offset);
        //System.out.println(index +"\t"+ dataSegmentSize);
        if (nextFree.get() < writeLimit) {
            buf.putLong(key.getMSL());
            buf.putLong(key.getLSL());
            buf.put((ByteBuffer)formattedBuf.duplicate().slice().limit(storedLength));
            //buf.put(formattedBuf.array(), formattedBuf.position(), storedLength);
        	return offset;
        } else {
            return writeFailedOffset;
        }
    }
    
    public static int writeToBuf(DHTKey key, ByteBuffer value, StorageParameters storageParams, byte[] userData, 
                              ByteBuffer buf, AtomicInteger nextFree, int writeLimit) {
        int offset;
        int writeSize;
        int storedLength;
        int compressedLength;
        int checksumLength;
        short   ccss;
        
        if (storageParams.compressedSizeSet()) {
            compressedLength = storageParams.getCompressedSize();
        } else {
            compressedLength = value.remaining();
        }
        
        // The client disallows any compression usage when the compressed size equals or exceeds the
        // uncompressed size. As ccss is per message, and this compression check is per value, 
        // we catch this case and modify the ccss for values where this applies.
        if (compressedLength == storageParams.getUncompressedSize()) {
            ccss = CCSSUtil.updateCompression(storageParams.getCCSS(), Compression.NONE);
        } else {
            ccss = storageParams.getCCSS();
        }
        
        checksumLength = storageParams.getChecksum().length;
        storedLength = MetaDataUtil.computeStoredLength(compressedLength, checksumLength, userData.length); 
        
        writeSize = storedLength + (key != null ? DHTKey.BYTES_PER_KEY : 0);
        //System.out.println("writeSize: "+ writeSize);
        offset = nextFree.getAndAdd(writeSize);
        Log.fine("offset: ", offset);
        //System.out.println(index +"\t"+ dataSegmentSize);
        if (nextFree.get() < writeLimit) {
            int         dataLength;
                        
            // write metadata
            // FUTURE - think about reducing redundancy in stored/compressed/userdata lengths
            
            //System.out.println("WRITING:\n"+ buf);
            if (key != null) {
	            buf.putLong(key.getMSL());
	            buf.putLong(key.getLSL());
            }
            
            buf.putInt(storedLength); // storedLength
            if (debug) {
                System.out.printf("compressedLength: %d\n", compressedLength);
                System.out.printf("checksumLength: %d\n", checksumLength);
                System.out.printf("userData.length: %d\n", userData.length);
                System.out.printf("storedLength: %d\n", storedLength);
                System.out.printf("storageParams.getUncompressedSize(): %d\n", storageParams.getUncompressedSize());
                System.out.printf("value: %s %s\n", value, StringUtil.byteBufferToHexString(value));
            }
            
            buf.putInt(storageParams.getUncompressedSize());
            buf.putLong(storageParams.getVersion());
            buf.putLong(storageParams.getCreationTime());
            buf.put(storageParams.getValueCreator());
            buf.putShort(ccss);
            buf.put((byte)userData.length);
            buf.put(storageParams.getChecksum());
            
            // write value
            if (debug) {
                System.out.printf("value %s remaining %d\n", value, value.remaining());
            }
            
            dataLength = value.remaining();
            buf.put(value.array(), value.position(), dataLength);
            
            // FIXME - enforce userdata length limit
            buf.put(userData, 0, userData.length);
    
            return offset;
        } else {
            return writeFailedOffset;
        }
    }
}
