package com.ms.silverking.cloud.dht.net;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.logging.Level;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.net.protocol.KeyedMessageFormat;
import com.ms.silverking.cloud.dht.net.protocol.RetrievalResponseMessageFormat;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;

/**
 * ProtoMessageGroup for messages that contains values
 */
public final class ProtoValueMessageGroup extends ProtoValueMessageGroupBase {
    private int expectedSize;
    // FUTURE - dedup wrt ProtoPutMessageGroup
    
    public ProtoValueMessageGroup(UUIDBase uuid, long context, int opSize, int valueBytes, byte[] originator, int deadlineRelativeMillis) {
        super(MessageType.RETRIEVE_RESPONSE, uuid, context, opSize, valueBytes, 
                ByteBuffer.allocate(RetrievalResponseMessageFormat.optionBytesSize), 
                    RetrievalResponseMessageFormat.size - KeyedMessageFormat.baseBytesPerKeyEntry, 
                    originator, deadlineRelativeMillis, ForwardingMode.FORWARD);
        expectedSize = valueBytes;
    }
    
    public void addValue(DHTKey dhtKey, ByteBuffer value, int compressedLength, boolean noCopy) {
        boolean copyValue;
        int     compressedValueSize;
        //int     uncompressedValueSize;
        int     storedValueSize;
        //byte    compression;

        /*
         // for debugging only
        if (value.isDirect() && (value.position() != 0 || value.limit() == 0)) {
            System.err.printf("addValue: %s %s %d\n", dhtKey, value, compressedLength);
            Thread.dumpStack();
            System.exit(-1);
        }
        */
        storedValueSize = value.remaining();
        
        //compressedValueSize = MetaDataUtil.getCompressedLength(value, value.position());
        compressedValueSize = compressedLength;
        totalValueBytes += storedValueSize;
        //uncompressedValueSize = MetaDataUtil.getUncompressedLength(value.array(), value.position());
        if (Log.levelMet(Level.FINE)) {
            Log.warning("storedValueSize: "+ storedValueSize);
            Log.warning("compressedValueSize: ", compressedValueSize);
            //Log.warning("uncompressedValueSize: ", uncompressedValueSize);
        }
        //compression = MetaDataUtil.getCompression(value.array(), value.position());
        // need to serialize the key
        addKey(dhtKey);
        /*
         * The question to be solved here is whether to copy the value or
         * to create a new bytebuffer entry in the messagegroup.
         * 
         * Leave in place when any of the following hold:
         *  a) single value
         *  b) large value
         *  
         * Copy when:
         *  a) many small values
         */
        if (noCopy) {
            copyValue = false;
        } else {
            if (opSize == 1) {
                copyValue = false;
            } else if (compressedValueSize >= dedicatedBufferSizeThreshold) {
                copyValue = false;
            } else {
                copyValue = true;
            }                
        }
        if (copyValue) {        
            if (valueBuffer == null || storedValueSize > valueBuffer.remaining()) {
                // in below line we don't need to consider compressedValueSize since
                // dedicatedBufferSizeThreshold <= valueBufferSize
                //valueBuffer = ByteBuffer.allocate(valueBufferSize);
                //valueBuffer = ByteBuffer.allocate(Math.min(expectedSize, valueBufferSize));
                valueBuffer = ByteBuffer.allocate(Math.max(expectedSize - totalValueBytes, storedValueSize));
                if (!addMultiValueBuffer(valueBuffer)) {
                    throw new RuntimeException("Too many buffers");
                }
            }
            // record where the value will be written into the key buffer
            keyByteBuffer.putInt(curMultiValueBufferIndex);
            keyByteBuffer.putInt(valueBuffer.position());
            //keyByteBuffer.putInt(uncompressedValueSize);
            //keyByteBuffer.putInt(compressedValueSize);
            keyByteBuffer.putInt(storedValueSize);
            //keyByteBuffer.put(compression);
            
            //System.out.printf("value       %s\n", value);
            //System.out.printf("valueBuffer %s\n", valueBuffer);
            //System.out.printf("valueBuffer.position() %d storedValueSize %d\n", valueBuffer.position(), storedValueSize);
            //System.arraycopy(value.array(), value.arrayOffset(), valueBuffer.array(), valueBuffer.position(), storedValueSize);
            try {
                value.get(valueBuffer.array(), valueBuffer.position(), storedValueSize);
            } catch (BufferUnderflowException bfe) {
                System.out.println(value.remaining() +"\t"+ storedValueSize);
                System.out.println(value);
                System.out.println(valueBuffer);
                throw bfe;
            }
            //System.out.println("\n");
            //System.out.println(value);
            //System.out.println(valueBuffer);
            //System.out.println(new String(valueBuffer.array()) +" "+ valueSize);
            //System.arraycopy(valueArray, valueOffset, valueBuffer.array(), valueBuffer.position(), valueSize);
            if (valueBuffer.remaining() > storedValueSize) {
                valueBuffer.position(valueBuffer.position() + storedValueSize);
            } else {
                assert valueBuffer.remaining() == storedValueSize;
                valueBuffer.position(valueBuffer.limit());
                valueBuffer = null;
            }
            //System.out.println(valueBuffer);
        } else {
            //ByteBuffer  newBuf;
            
            // record where the value will be located in the key buffer
            keyByteBuffer.putInt(bufferList.size());
            keyByteBuffer.putInt(value.position());
            //keyByteBuffer.putInt(uncompressedValueSize);
            //keyByteBuffer.putInt(compressedValueSize);
            keyByteBuffer.putInt(storedValueSize);
            //keyByteBuffer.put(compression);
            // FUTURE - think about removing the need for size since strictly speaking
            // it isn't necessary
            
            //newBuf = ByteBuffer.wrap(valueArray, valueOffset, valueSize);
            //newBuf.position(newBuf.position() + valueSize);
            //bufferList.add(newBuf);
            /*
            if (valueBuffer.remaining() > storedValueSize) {
                try {
                    valueBuffer.position(value.position() + storedValueSize);
                } catch (RuntimeException re) { // TEMP DEBUG
                    System.out.println(value +"\t"+ storedValueSize);
                    throw re;
                }
            } else {
                value.position(value.limit());
                valueBuffer = null;
            }
            */
            value.position(value.limit());
            //System.out.println(value);
            //System.out.println(new String(value.array()));
            bufferList.add(value);
        }
        //displayForDebug();
    }

    public void addErrorCode(DHTKey key, OpResult result) {
        addKey(key);
        keyByteBuffer.putInt(-(result.ordinal() + 1));
        keyByteBuffer.putInt(-1);
        keyByteBuffer.putInt(-1);
    }
}
