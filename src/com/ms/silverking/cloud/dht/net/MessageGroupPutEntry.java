package com.ms.silverking.cloud.dht.net;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.net.protocol.PutMessageFormat;

public class MessageGroupPutEntry extends MessageGroupKVEntry {
    private final int           uncompressedLength;
    private final ChecksumType  checksumType;
    private final byte[]        checksum;

    public MessageGroupPutEntry(ByteBuffer keyBuffer, int offset, ByteBuffer[] buffers,
                                ChecksumType checksumType) {
        super(keyBuffer, offset);        
        this.checksumType = checksumType;
        uncompressedLength = keyBuffer.getInt(offset + PutMessageFormat.uncompressedValueLengthOffset);
        storedLength = keyBuffer.getInt(offset + PutMessageFormat.compressedValueLengthOffset);
        checksum = new byte[checksumType.length()];
        System.arraycopy(keyBuffer.array(), offset + PutMessageFormat.checksumOffset, checksum, 0, checksum.length); // FIXME - delete
        // FIXME - CHECK ABOVE LINE
        initValBuffer(buffers);
    }
    
    public int getUncompressedLength() {
        return uncompressedLength;
    }
    
    public int entryLength() {
        return PutMessageFormat.size(checksumType);
    }
    
    public byte[] getChecksum() {
        return checksum;
    }
}
