package com.ms.silverking.cloud.dht.net;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.id.UUIDBase;

public class ProtoOpResponseMessageGroup extends ProtoMessageGroup {
    private final ByteBuffer    reponseByteBuffer;
    
    private static final int    dataBufferIndex = 0;
    
    // room for response code
    private static final int    responseBufferSize = 1;
    private static final int	resultIndex = 0;
    
    public ProtoOpResponseMessageGroup(UUIDBase uuid, long context, OpResult result, byte[] originator, int deadlineRelativeMillis) {
        super(MessageType.OP_RESPONSE, uuid, context, originator, deadlineRelativeMillis, ForwardingMode.FORWARD);
        
        reponseByteBuffer = ByteBuffer.allocate(responseBufferSize);
        bufferList.add(reponseByteBuffer);
        reponseByteBuffer.put((byte)result.ordinal());
    }

    @Override
    public boolean isNonEmpty() {
        return true;
    }
    
    public static OpResult result(MessageGroup mg) {
        return OpResult.values()[mg.getBuffers()[dataBufferIndex].get(resultIndex)];
    }        
}
