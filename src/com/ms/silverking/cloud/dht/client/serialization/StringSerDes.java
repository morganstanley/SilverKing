package com.ms.silverking.cloud.dht.client.serialization;

import java.nio.ByteBuffer;

/**
 * Serializer/deserializer for String
 */
public final class StringSerDes implements BufferSerDes<String> {
    @Override 
    public ByteBuffer serializeToBuffer(String s) {
		return ByteBuffer.wrap(s.getBytes());
    }
    
    @Override
    public void serializeToBuffer(String s, ByteBuffer buffer) {
		buffer.put(s.getBytes());
    }
    
    @Override
    public int estimateSerializedSize(String s) {
		return s.getBytes().length; // FUTURE - think about this
    }

	@Override
	public String deserialize(ByteBuffer[] buffers) {
	    return new String(ByteArraySerDes.deserializeBuffers(buffers));
	}

    @Override
    public String deserialize(ByteBuffer buffer) {
        return new String(buffer.array(), buffer.position(), buffer.remaining());
    }

	@Override
	public String emptyObject() {
		return "";
	}
}
