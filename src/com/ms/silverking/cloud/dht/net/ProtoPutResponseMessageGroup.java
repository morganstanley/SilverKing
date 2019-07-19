package com.ms.silverking.cloud.dht.net;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.net.protocol.PutResponseMessageFormat;
import com.ms.silverking.id.UUIDBase;

public class ProtoPutResponseMessageGroup extends ProtoKeyedMessageGroup {
    private static final int    keyBufferAdditionalBytesPerKey = 1;
    private static final int    optionsBufferSize = PutResponseMessageFormat.optionBytesSize;
    
    // FUTURE - is there a better way to compute the 1 index difference
    private static final int  optionBufferIndex = 0; // on reception header is removed
    
    public ProtoPutResponseMessageGroup(UUIDBase uuid, long context, long version, int numKeys, byte[] originator, byte storageState, int deadlineRelativeMillis) {
        super(MessageType.PUT_RESPONSE, uuid, context, 
                ByteBuffer.allocate(optionsBufferSize), numKeys, keyBufferAdditionalBytesPerKey, 
                originator, deadlineRelativeMillis, ForwardingMode.FORWARD);
        bufferList.add(0, optionsByteBuffer);
        optionsByteBuffer.putLong(version);
        optionsByteBuffer.put(storageState);
        //System.out.printf("optionsByteBuffer\t%s %x %x\n", 
        //  StringUtil.byteBufferToHexString(optionsByteBuffer), version, storageState);
    }

    public void addResult(DHTKey key, OpResult result) {
        addKey(key);
        keyByteBuffer.put((byte)result.ordinal());
    }
    
    public static byte getStorageState(MessageGroup mg) {
        return mg.getBuffers()[optionBufferIndex].get(PutResponseMessageFormat.storageStateOffset);
    }
}
