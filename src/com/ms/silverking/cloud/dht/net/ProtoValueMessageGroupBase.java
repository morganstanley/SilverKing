package com.ms.silverking.cloud.dht.net;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.client.impl.SegmentationUtil;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.net.protocol.KeyValueMessageFormat;
import com.ms.silverking.id.UUIDBase;

/**
 * ProtoMessageGroup for messages that contains values e.g. put and retrieval response 
 */
abstract class ProtoValueMessageGroupBase extends ProtoKeyedMessageGroup {
    protected final int   opSize;
    protected ByteBuffer  valueBuffer;
    protected int      	  curMultiValueBufferIndex;
    protected int         totalValueBytes;
    
    // FIXME - valueBufferSize should be computed based on some
    // external information
    protected static final int   valueBufferSize = 16 * 1024;
    public static final int   maxValueBytesPerMessage = SegmentationUtil.maxValueSegmentSize + 1024 * 1024;
    private static final int     extraMargin = 1024;
    protected static final int   dedicatedBufferSizeThreshold = 16 * 1024;    
    
    protected static final int  optionBufferIndex = KeyValueMessageFormat.optionBufferIndex;
        
    static {
        if (maxValueBytesPerMessage + extraMargin <= SegmentationUtil.maxValueSegmentSize) {
            throw new RuntimeException("maxValueBytesPerMessage + extraMargin <= SegmentationUtil.maxValueSegmentSize is not supported");
        }
        if (dedicatedBufferSizeThreshold > valueBufferSize) {
            throw new RuntimeException("dedicatedBufferSizeThreshold > valueBufferSize not supported");
        }
    }
    
    public ProtoValueMessageGroupBase(MessageType type, UUIDBase uuid, long context, 
                                      int opSize, int valueBytes, 
                                      ByteBuffer optionsByteBuffer, int additionalBytesPerKey, 
                                      byte[] originator, int deadlineRelativeMillis, ForwardingMode forward) {
        super(type, uuid, context, optionsByteBuffer, opSize, additionalBytesPerKey, originator, deadlineRelativeMillis, forward);
        this.opSize = opSize;
        bufferList.add(optionsByteBuffer);
        /*
        if (opSize > 1) {
            if (valueBytes > 0) {
                //valueBuffer = ByteBuffer.allocate(dedicatedBufferSizeThreshold);
                valueBuffer = ByteBuffer.allocate(valueBytes);
                // FIXME - above may be switched to allocate direct
                // However, that requires a careful think through all copying
                // and direct buffer usage
                //curMultiValueBufferIndex = (short)bufferList.size();
                //bufferList.add(valueBuffer);
                addMultiValueBuffer(valueBuffer);
            }
        } else {
            valueBuffer = null;
            curMultiValueBufferIndex = -1;
        }
        */
        valueBuffer = null;
        curMultiValueBufferIndex = -1;
    }
    
    public boolean canBeAdded(int valueSize) {
        return totalValueBytes + valueSize < maxValueBytesPerMessage;
    }
    
    public int currentValueBytes() {
        return totalValueBytes;
    }
    
    protected boolean addDedicatedBuffer(ByteBuffer buffer) {
        if (bufferList.size() >= Integer.MAX_VALUE) {
            return false;
        } else {
            bufferList.add(buffer);
            return true;
        }
    }
    
    protected boolean addMultiValueBuffer(ByteBuffer buffer) {
        if (bufferList.size() >= Integer.MAX_VALUE) {
            return false;
        } else {
            curMultiValueBufferIndex = bufferList.size(); 
            bufferList.add(buffer);
            return true;
        }
    }        
    
    public void addErrorCode(DHTKey key) {
        addKey(key);
        keyByteBuffer.putInt(-1);
        keyByteBuffer.putInt(-1);
        keyByteBuffer.putInt(-1);
    }
}
