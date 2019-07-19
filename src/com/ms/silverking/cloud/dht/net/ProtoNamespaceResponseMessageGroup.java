package com.ms.silverking.cloud.dht.net;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.numeric.NumConversion;

public class ProtoNamespaceResponseMessageGroup extends ProtoMessageGroup {
    private final ByteBuffer    dataByteBuffer;
    
    private static final int    dataBufferIndex = 0;
    // room for UUID
    private static final int    dataBufferSize = 2 * NumConversion.BYTES_PER_LONG;
    private static final int    uuidMSLOffset = 0;
    private static final int    uuidLSLOffset = uuidMSLOffset + NumConversion.BYTES_PER_LONG;
    private static final int    nsListOffset = uuidLSLOffset + NumConversion.BYTES_PER_LONG;
    private static final int    nsListDataOffset = nsListOffset + NumConversion.BYTES_PER_INT;
    
    private static final int    deadlineRelativeMillis = 10 * 60 * 1000;
    private static final long   nsRequestMGContext = 0L;    
    
    public ProtoNamespaceResponseMessageGroup(UUIDBase uuid, byte[] originator, List<Long> namespaces) {
        super(MessageType.NAMESPACE_RESPONSE, uuid, nsRequestMGContext, originator, 
              deadlineRelativeMillis, ForwardingMode.FORWARD);
        
        dataByteBuffer = ByteBuffer.allocate(dataBufferSize + bufferSizeFor(namespaces));
        bufferList.add(dataByteBuffer);
        dataByteBuffer.putLong(uuid.getMostSignificantBits());
        dataByteBuffer.putLong(uuid.getLeastSignificantBits());
        dataByteBuffer.putInt(namespaces.size());
        for (Long ns : namespaces) {
            dataByteBuffer.putLong(ns);
        }
    }
    
    private int bufferSizeFor(List<Long> namespaces) {
        // integer for length of list + one long per list entry
        return NumConversion.BYTES_PER_INT + NumConversion.BYTES_PER_LONG * namespaces.size();
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
    
    public static List<Long> getNamespaces(MessageGroup mg) {
        int         nsListSize;
        List<Long>  namespaces;
        
        nsListSize = mg.getBuffers()[dataBufferIndex].getInt(nsListOffset); 
        namespaces = new ArrayList<>(nsListSize);
        for (int i = 0; i < nsListSize; i++) {
            namespaces.add(mg.getBuffers()[dataBufferIndex].getLong(nsListDataOffset + i * NumConversion.BYTES_PER_LONG)); 
        }
        return namespaces;
    }
}
