package com.ms.silverking.cloud.dht.net;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.net.protocol.KeyedMessageFormat;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.numeric.NumConversion;

/**
 * ProtoMessageGroup for keyed messages 
 */
public abstract class ProtoKeyedMessageGroup extends ProtoMessageGroup {
    private final int   keyBufferAdditionalBytesPerKey;
    protected ByteBuffer  keyByteBuffer;
    protected final ByteBuffer  optionsByteBuffer;
    private int totalKeys;
    
    private static final int    keyBufferExpansionKeys = 32;

    public ProtoKeyedMessageGroup(MessageType type, UUIDBase uuid, long context, 
            ByteBuffer optionsByteBuffer, int numKeys, int keyBufferAdditionalBytesPerKey, 
            byte[] originator, int deadlineRelativeMillis, ForwardingMode forward) {
        super(type, uuid, context, originator, deadlineRelativeMillis, forward);
        
        this.keyBufferAdditionalBytesPerKey = keyBufferAdditionalBytesPerKey;
        keyByteBuffer = allocateKeyBuffer(numKeys, keyBufferAdditionalBytesPerKey);
        
        //System.out.println("keyBufferSize: "+ keyBufferSize 
        //        +"\tkeyBufferAdditionalBytesPerKey: "+ keyBufferAdditionalBytesPerKey);
        
        bufferList.add(keyByteBuffer);

        this.optionsByteBuffer = optionsByteBuffer;
        // added to list by children
    }
    
    private static final ByteBuffer allocateKeyBuffer(int numKeys, int keyBufferAdditionalBytesPerKey) {
        ByteBuffer  keyBuffer;
        int         bytesPerEntry;
        
        bytesPerEntry = KeyedMessageFormat.baseBytesPerKeyEntry + keyBufferAdditionalBytesPerKey;
        keyBuffer = ByteBuffer.allocate(NumConversion.BYTES_PER_SHORT + bytesPerEntry * numKeys);
        keyBuffer.putShort((short)bytesPerEntry);
        return keyBuffer;
    }
    
    public int currentBufferKeys() {
        return totalKeys;
    }
    
    public void addKey(DHTKey dhtKey) {
        try {
            ++totalKeys;
            keyByteBuffer.putLong(dhtKey.getMSL());
            keyByteBuffer.putLong(dhtKey.getLSL());
        } catch (BufferOverflowException bfe) {
            ByteBuffer  newKeyByteBuffer;
            
            Log.fine("ProtoKeyedMessageGroup keyByteBuffer overflow. Expanding.");
            newKeyByteBuffer = allocateKeyBuffer(currentBufferKeys() + keyBufferExpansionKeys, keyBufferAdditionalBytesPerKey);
            keyByteBuffer.flip();
            newKeyByteBuffer.put(keyByteBuffer);
            keyByteBuffer = newKeyByteBuffer;
            keyByteBuffer.putLong(dhtKey.getMSL());
            keyByteBuffer.putLong(dhtKey.getLSL());
        }
    }
    
    public boolean isNonEmpty() {
        return keyByteBuffer.position() != 0;
    }
}
