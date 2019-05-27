package com.ms.silverking.cloud.dht.net;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.numeric.NumConversion;

public class ProtoByteArrayMessageGroup extends ProtoMessageGroup {
    private final ByteBuffer    msgByteBuffer;
    
    private static final int    dataBufferIndex = 0;
    
    // room for length (byte array length added to this in constructor)
    private static final int    msgBufferSize = NumConversion.BYTES_PER_INT;
    private static final int	dataLengthOffset = 0;
    private static final int	dataOffset = dataLengthOffset + NumConversion.BYTES_PER_INT;
    
    public ProtoByteArrayMessageGroup(MessageType type, UUIDBase uuid, long context, OpResult result, byte[] originator, int deadlineRelativeMillis, byte[] data) {
        super(type, uuid, context, originator, deadlineRelativeMillis, ForwardingMode.FORWARD);
        
        msgByteBuffer = ByteBuffer.allocate(msgBufferSize + data.length);
        bufferList.add(msgByteBuffer);
        msgByteBuffer.putInt(data.length);
        msgByteBuffer.put(data);
    }

    @Override
    public boolean isNonEmpty() {
        return true;
    }
    
    public static byte[] getData(MessageGroup mg) {
    	byte[]	data;
    	int		length;
    	
    	length = mg.getBuffers()[dataBufferIndex].getInt(dataLengthOffset);
    	data = new byte[length];
    	BufferUtil.get(mg.getBuffers()[dataBufferIndex], dataOffset, data, data.length);
    	return data;
    }
}
