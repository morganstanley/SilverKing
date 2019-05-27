package com.ms.silverking.cloud.dht.net;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.common.EnumValues;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.net.protocol.RetrievalResponseMessageFormat;

public class MessageGroupRetrievalResponseEntry extends MessageGroupKVEntry {
    public MessageGroupRetrievalResponseEntry(ByteBuffer keyBuffer, int offset, ByteBuffer[] buffers) {
        super(keyBuffer, offset);
        storedLength = keyBuffer.getInt(offset + RetrievalResponseMessageFormat.resultLengthOffset);
        initValBuffer(buffers);
    }
    
    public int entryLength() {
        return RetrievalResponseMessageFormat.size;
    }
    
    public OpResult getOpResult() {
        return hasValue() ? OpResult.SUCCEEDED : EnumValues.opResult[getErrorCode()];
    }
}
