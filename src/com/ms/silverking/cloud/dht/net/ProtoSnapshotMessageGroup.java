package com.ms.silverking.cloud.dht.net;

import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.id.UUIDBase;

// Consider removing this class (if snapshots are removed)
public class ProtoSnapshotMessageGroup extends ProtoVersionedBasicOpMessageGroup {
    /*
     * FUTURE - delete this when verified/tested
     * 
    private final ByteBuffer    dataByteBuffer;
    
    private static final int    dataBufferIndex = 0;
    // room for UUID + version
    private static final int    dataBufferSize = 3 * NumConversion.BYTES_PER_LONG;
    private static final int    uuidMSLOffset = 0;
    private static final int    uuidLSLOffset = uuidMSLOffset + NumConversion.BYTES_PER_LONG;
    private static final int    versionOffset = uuidLSLOffset + NumConversion.BYTES_PER_LONG;
    */
    
    public ProtoSnapshotMessageGroup(MessageType type, UUIDBase uuid, long context, long version, byte[] originator) {
        super(type, uuid, context, version, originator);
      /*  
        dataByteBuffer = ByteBuffer.allocate(dataBufferSize);
        bufferList.add(dataByteBuffer);
        dataByteBuffer.putLong(uuid.getMostSignificantBits());
        dataByteBuffer.putLong(uuid.getLeastSignificantBits());
        dataByteBuffer.putLong(version);
        */
    }

    @Override
    public boolean isNonEmpty() {
        return true;
    }
    
    /*
    public static long getUUIDMSL(MessageGroup mg) {
        return mg.getBuffers()[dataBufferIndex].getLong(uuidMSLOffset);
    }    
    
    public static long getUUIDLSL(MessageGroup mg) {
        return mg.getBuffers()[dataBufferIndex].getLong(uuidLSLOffset);
    }
    
    public static long getVersion(MessageGroup mg) {
        return mg.getBuffers()[dataBufferIndex].getLong(versionOffset);
    }
    */    
}
