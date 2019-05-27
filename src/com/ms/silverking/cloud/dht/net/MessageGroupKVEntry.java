package com.ms.silverking.cloud.dht.net;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.net.protocol.KeyValueMessageFormat;
import com.ms.silverking.text.StringUtil;

/**
 * Parent used by both MessageGroupPutEntry and MessageGroupRetrievalResponseEntry.
 * This code avoids copying the data from the generating message by
 * creating new ByteBuffers that are views into the original buffers.
 */
public abstract class MessageGroupKVEntry extends MessageGroupKeyEntry {
    private final int	bufferIndex;
    private final int   bufferOffset;
    protected int       storedLength;
    
    protected ByteBuffer    valBuffer;
    
    private static final boolean    debug = false;
    
    public MessageGroupKVEntry(ByteBuffer keyBuffer, int offset) {
        super(keyBuffer, offset);
        bufferIndex = keyBuffer.getInt(offset + KeyValueMessageFormat.bufferIndexOffset);
        bufferOffset = keyBuffer.getInt(offset + KeyValueMessageFormat.bufferOffsetOffset);
    }
    
    protected void initValBuffer(ByteBuffer[] buffers) {
        if (bufferIndex >= 0) {
            try {
                // FIXME - NEED TO LOOK INTO USAGE OF NATIVE BUFFERS
                if (debug) {
                    System.out.println("dbg1\t"+ bufferIndex +" "+ bufferOffset +" "+ storedLength);
                    System.out.println("dbg1.b\t"+ buffers[bufferIndex]);
                    System.out.println("dbg1.c\t"+ buffers[bufferIndex].array().length);
                    System.out.flush();
                    if (bufferOffset + storedLength > buffers[bufferIndex].array().length) {
                        throw new RuntimeException("buffer overrun");
                    }
                }
                
                if (buffers[bufferIndex].isDirect()) {
                //if (buffers[bufferIndex].isDirect() || buffers[bufferIndex].isReadOnly()) { // second clause is TEMP
                // Think about why we can't use for read only. Ideally we would like to reduce copies
                    int     length;
                    
                    length = storedLength;
                    valBuffer = (ByteBuffer)((ByteBuffer)buffers[bufferIndex].duplicate().position(bufferOffset)).slice().limit(length);                    
                } else {
                    byte[]  array;
                    int     length;
                    
                    array = buffers[bufferIndex].array();
                    length = storedLength;
                    valBuffer = ByteBuffer.wrap(array, // FUTURE - this is using array, consider buffer
                                    bufferOffset,
                                    length);
                                    //Math.min(array.length, length));
                }
                if (debug) {
                    System.out.println("dbg1.z\t"+ valBuffer);
                }
            } catch (RuntimeException re) {
                byte[]  array;
                
                re.printStackTrace();
                System.out.println(super.toString());
                System.out.printf("bufferIndex %d  bufferOffset %d  storedLength %d\n",
                                    bufferIndex, bufferOffset, storedLength);
                System.out.println(buffers.length);
                System.out.println(buffers[bufferIndex]);
                System.out.println(buffers[bufferIndex].array().length);
                array = buffers[bufferIndex].array();
                System.out.println(storedLength);
                System.out.println();
                for (int i = 0; i < buffers.length; i++) {
                    System.out.println(i +"\t"+ buffers[i]);
                    System.out.println(StringUtil.byteBufferToHexString(buffers[i]));
                }
                System.out.println("..........");
                throw re;
            }
        } else {
            valBuffer = null;
        }
    }
    
    public int getBufferIndex() {
        return bufferIndex;
    }

    public int getBufferOffset() {
        return bufferOffset;
    }
    
    public int getStoredLength() {
        return storedLength;
    }
    
    public ByteBuffer getValue() {
        return valBuffer;
    }
    
    public boolean hasValue() {
        return valBuffer != null;
    }
    
    public int getErrorCode() {
        if (bufferIndex >= 0) {
            throw new RuntimeException("bufferIndex "+ bufferIndex);
        } else {
            return -(bufferIndex + 1);
        }
    }
    
    public int entryLength() {
        return bytesPerEntry;
    }
    
    public int getValueLength() {
        return valBuffer != null ? valBuffer.limit() : 0;
    }
    
    @Override
    public String toString() {
        return super.toString() +":"+ bufferIndex +":"+ bufferOffset +":"+ storedLength;
    }
}
