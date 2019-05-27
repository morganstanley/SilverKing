package com.ms.silverking.cloud.dht.client.serialization;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.io.util.BufferUtil;

/**
 * Serializer/deserializer for byte[].
 */
public final class ByteArraySerDes implements BufferSerDes<byte[]> {
    @Override
    public ByteBuffer serializeToBuffer(byte[] b) {
        return ByteBuffer.wrap(Arrays.copyOf(b, b.length));
    }

    @Override
    public void serializeToBuffer(byte[] b, ByteBuffer buffer) {
        buffer.put(b);
    }

    @Override
    public int estimateSerializedSize(byte[] b) {
        return b.length;
    }

    public static byte[] deserializeBuffers(ByteBuffer[] buffers) {
        int     totalRemaining;
        byte[]  array;
        int     totalDeserialized;
        
        totalRemaining = BufferUtil.totalRemaining(buffers);
        array = new byte[totalRemaining];
        totalDeserialized = 0;
        for (ByteBuffer buffer : buffers) {
            int bufferRemaining;
            
            bufferRemaining = buffer.remaining();
            buffer.get(array, totalDeserialized, bufferRemaining);
            totalDeserialized += bufferRemaining;
        }
        return array;
    }
    
    @Override
    public byte[] deserialize(ByteBuffer[] buffers) {
        return deserializeBuffers(buffers);
    }

    @Override
    public byte[] deserialize(ByteBuffer buffer) {
        // FUTURE - remove array usage to allow for native buffering
        return Arrays.copyOfRange(buffer.array(), buffer.position(), buffer.position() + buffer.remaining());
    }

	@Override
	public byte[] emptyObject() {
		return DHTConstants.emptyByteArray;
	}
}
