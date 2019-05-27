package com.ms.silverking.cloud.dht.net;

import java.nio.ByteBuffer;
import java.util.List;

import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.numeric.NumConversion;

public class ProtoVersionedBasicOpMessageGroup extends ProtoMessageGroup {
    private boolean isNonEmpty;
    
    private static final int    defaultDeadlineRelativeMillis = 60 * 1000;
    
    private static final int    dataBufferIndex = 0;
    // room for UUID + version
    private static final int    uuidMSLOffset = 0;
    private static final int    uuidLSLOffset = uuidMSLOffset + NumConversion.BYTES_PER_LONG;
    private static final int    versionOffset = uuidLSLOffset + NumConversion.BYTES_PER_LONG;
    
    public ProtoVersionedBasicOpMessageGroup(MessageType type, UUIDBase uuid, long context, long version, byte[] originator) {
        super(type, uuid, context, originator, defaultDeadlineRelativeMillis, ForwardingMode.DO_NOT_FORWARD);
        
        ByteBuffer      buffer;
        
        buffer = ByteBuffer.allocate(3 * NumConversion.BYTES_PER_LONG);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getUUID().getLeastSignificantBits());
        buffer.putLong(version);
        buffer.flip();
        bufferList.add(buffer);
    }
    
    public MessageGroup toMessageGroup() {
        return toMessageGroup(false);
    }
    
    public void addToMessageGroupList(List<MessageGroup> messageGroups) {
        super.addToMessageGroupList(messageGroups);
    }
    
    @Override
    public boolean isNonEmpty() {
        return isNonEmpty;
    }
    
    public void setNonEmpty() {
        isNonEmpty = true;
    }
    
    /////////////////////
    
    public static long getUUIDMSL(MessageGroup mg) {
        return mg.getBuffers()[dataBufferIndex].getLong(uuidMSLOffset);
    }    
    
    public static long getUUIDLSL(MessageGroup mg) {
        return mg.getBuffers()[dataBufferIndex].getLong(uuidLSLOffset);
    }
    
    public static long getVersion(MessageGroup mg) {
        return mg.getBuffers()[dataBufferIndex].getLong(versionOffset);
    }    
}
