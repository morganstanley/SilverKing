package com.ms.silverking.cloud.dht.client.serialization;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.id.UUIDUtil;

/**
 * Serializer/deserializer for UUID
 */
public final class UUIDSerDes implements BufferSerDes<UUID> {
    @Override 
    public ByteBuffer serializeToBuffer(UUID uuid) {
        return ByteBuffer.wrap(UUIDUtil.uuidToBytes(uuid));
    }
    
    @Override
    public void serializeToBuffer(UUID uuid, ByteBuffer buffer) {
        buffer.put(UUIDUtil.uuidToBytes(uuid));
    }
    
    @Override
    public int estimateSerializedSize(UUID uuid) {
        return UUIDUtil.BYTES_PER_UUID;
    }

	@Override
	public UUID deserialize(ByteBuffer[] buffers) {
	    byte[] def;
	    
	    def = ByteArraySerDes.deserializeBuffers(buffers);
	    if (def.length != UUIDUtil.BYTES_PER_UUID) {
	        throw new RuntimeException("Unable to deserialize UUID def length: "+ def.length);
	    }
	    return UUIDUtil.bytesToUUID(def);
	}

    @Override
    public UUID deserialize(ByteBuffer buffer) {
        return UUIDUtil.getUUID(buffer);
    }

	@Override
	public UUID emptyObject() {
		return new UUIDBase(0, 0).getUUID();
	}
}
