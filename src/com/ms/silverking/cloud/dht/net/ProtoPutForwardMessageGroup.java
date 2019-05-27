package com.ms.silverking.cloud.dht.net;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.logging.Level;

import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.net.protocol.KeyedMessageFormat;
import com.ms.silverking.cloud.dht.net.protocol.PutMessageFormat;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;

/**
 * ProtoMessageGroup for forwarded put messages 
 */
public final class ProtoPutForwardMessageGroup extends ProtoValueMessageGroupBase {
    public ProtoPutForwardMessageGroup(UUIDBase uuid, long context,
            byte[] originator, ByteBuffer optionsByteBuffer,
            List<MessageGroupKeyEntry> destEntries, ChecksumType checksumType, int deadlineRelativeMillis) {
        super(MessageType.PUT, uuid, context, destEntries.size(), 
                totalLength(destEntries), 
                (ByteBuffer)optionsByteBuffer.asReadOnlyBuffer(), PutMessageFormat.size(checksumType) - KeyedMessageFormat.baseBytesPerKeyEntry, 
                originator, deadlineRelativeMillis, ForwardingMode.DO_NOT_FORWARD);
        if (debug) {
            System.out.println("\toptionsByteBuffer "+ optionsByteBuffer +"\t last "+ (bufferList.size() - 1));
        }
        for (int i = 0; i < destEntries.size(); i++) {
            MessageGroupPutEntry entry;
            
            entry = (MessageGroupPutEntry)destEntries.get(i);
            addValue(entry, entry);
        }
    }
    
    private static int totalLength(List<MessageGroupKeyEntry> destEntries) {
        int totalLength;
        
        totalLength = 0;
        for (MessageGroupKeyEntry entry : destEntries) {
            totalLength += ((MessageGroupPutEntry)entry).getStoredLength();
        }
        return totalLength;
    }
    
    public void addValue(DHTKey dhtKey, MessageGroupPutEntry entry) {
        boolean copyValue;
        int     compressedValueSize;
        int     uncompressedValueSize;
        int     storedValueSize;
        //byte    compression;
        
        ByteBuffer  value;
        
        if (debug) {
            System.out.println("entry: "+ entry);
            System.out.println("entry.getValue(): "+ entry.getValue());
        }
        value = entry.getValue().asReadOnlyBuffer();
        if (debug) {
            System.out.println("v0: "+ value);
        }
        //value.rewind();
        //System.out.println("v1: "+ value);
        
        //System.out.println(StringUtil.byteArrayToHexString(value.array(), 0, 128));
        
        storedValueSize = value.remaining();
        
        compressedValueSize = entry.getStoredLength();
        totalValueBytes += storedValueSize;
        uncompressedValueSize = entry.getUncompressedLength();
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
        if (opSize == 1) {
            copyValue = false;
        } else if (compressedValueSize >= dedicatedBufferSizeThreshold) {
            copyValue = false;
        } else {
            copyValue = true;
        }                
        if (copyValue) {        
            if (valueBuffer == null || storedValueSize > valueBuffer.remaining()) {
                // in below line we don't need to consider compressedValueSize since
                // dedicatedBufferSizeThreshold <= valueBufferSize
                valueBuffer = ByteBuffer.allocate(valueBufferSize);
                //if (bufferList.size() >= Short.MAX_VALUE) {
                //    throw new RuntimeException("Too many buffers");
                //}
                curMultiValueBufferIndex = bufferList.size(); 
                bufferList.add(valueBuffer);
            }
            // record where the value will be written into the key buffer
            //keyByteBuffer.putShort(curMultiValueBufferIndex);
            keyByteBuffer.putInt(curMultiValueBufferIndex);
            keyByteBuffer.putInt(valueBuffer.position());
            keyByteBuffer.putInt(uncompressedValueSize);
            keyByteBuffer.putInt(compressedValueSize);
            keyByteBuffer.put(entry.getChecksum());
            //keyByteBuffer.put(compression);
            
            //System.arraycopy(value.array(), value.arrayOffset(), valueBuffer.array(), valueBuffer.position(), storedValueSize);
            value.get(valueBuffer.array(), valueBuffer.position(), storedValueSize);
            //System.out.println("\n");
            //System.out.println(value);
            //System.out.println(valueBuffer);
            //System.out.println(new String(valueBuffer.array()) +" "+ valueSize);
            //System.arraycopy(valueArray, valueOffset, valueBuffer.array(), valueBuffer.position(), valueSize);
            valueBuffer.position(valueBuffer.position() + storedValueSize);
            //System.out.println(valueBuffer);
        } else {
            //ByteBuffer  newBuf;
            
            //if (bufferList.size() >= Short.MAX_VALUE) {
            //    throw new RuntimeException("Too many buffers");
            //}
            // record where the value will be located in the key buffer
            //keyByteBuffer.putShort((short)bufferList.size());
            keyByteBuffer.putInt(bufferList.size());
            keyByteBuffer.putInt(value.position());
            keyByteBuffer.putInt(uncompressedValueSize);
            keyByteBuffer.putInt(compressedValueSize);
            keyByteBuffer.put(entry.getChecksum());
            //keyByteBuffer.put(compression);
            // FIXME - think about removing the need for size since strictly speaking
            // it isn't necessary
            
            //newBuf = ByteBuffer.wrap(valueArray, valueOffset, valueSize);
            //newBuf.position(newBuf.position() + valueSize);
            //bufferList.add(newBuf);
            value.position(value.position() + storedValueSize);
            //System.out.println(value);
            //System.out.println(new String(value.array()));
            bufferList.add(value);
        }
        //displayForDebug();
    }
    
    public MessageGroup toMessageGroup() {
        for (int i = 0; i < bufferList.size(); i++) {
            if (i != optionBufferIndex) {
                bufferList.get(i).flip();
            }
        }
        return toMessageGroup(false);
    }
}
