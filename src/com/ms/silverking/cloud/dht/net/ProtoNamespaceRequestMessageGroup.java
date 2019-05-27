package com.ms.silverking.cloud.dht.net;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.numeric.NumConversion;

public class ProtoNamespaceRequestMessageGroup extends ProtoMessageGroup {
    private final ByteBuffer    dataByteBuffer;
    
    private static final int    dataBufferIndex = 0;
    // room for UUID
    private static final int    dataBufferSize = 2 * NumConversion.BYTES_PER_LONG;
    private static final int    uuidMSLOffset = 0;
    private static final int    uuidLSLOffset = uuidMSLOffset + NumConversion.BYTES_PER_LONG;
    
    private static final int    deadlineRelativeMillis = 10 * 60 * 1000;
    private static final long   nsRequestMGContext = 0L;    
    
    public ProtoNamespaceRequestMessageGroup(UUIDBase uuid, byte[] originator) {
        super(MessageType.NAMESPACE_REQUEST, uuid, nsRequestMGContext, originator, 
              deadlineRelativeMillis, ForwardingMode.FORWARD);
        
        dataByteBuffer = ByteBuffer.allocate(dataBufferSize);
        bufferList.add(dataByteBuffer);
        dataByteBuffer.putLong(uuid.getMostSignificantBits());
        dataByteBuffer.putLong(uuid.getLeastSignificantBits());
    }
    
    @Override
    public boolean isNonEmpty() {
        return true;
    }
    
    public static long getUUIDMSL(MessageGroup mg) {
        return mg.getBuffers()[dataBufferIndex].getLong(uuidMSLOffset);
    }    
    
    public static long getUUIDLSL(MessageGroup mg) {
        return mg.getBuffers()[dataBufferIndex].getLong(uuidLSLOffset);
    }
}
