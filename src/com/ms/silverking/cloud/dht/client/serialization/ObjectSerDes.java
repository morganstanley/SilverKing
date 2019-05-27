package com.ms.silverking.cloud.dht.client.serialization;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.ms.silverking.object.ObjectUtil;


/**
 * Native Java serializer/deserializer for generic Java objects. 
 * With this serialization, interaction is limited to other Java clients.
 *
 */
public final class ObjectSerDes implements BufferSerDes<Object> {
    @Override
    public Object deserialize(ByteBuffer[] buffers) {
        byte[]  _bytes;
        
        _bytes = ByteArraySerDes.deserializeBuffers(buffers);
        try {
            return ObjectUtil.bytesToObj(_bytes);
        } catch (Exception e) {
            throw new RuntimeException("Unable to deserialize", e);
        }
    }

    @Override
    public Object deserialize(ByteBuffer bytes) {
        byte[]  _bytes;
        
        // FUTURE - remove this copy
        _bytes = new byte[bytes.limit() - bytes.position()];
        bytes.get(_bytes);
        try {
            return ObjectUtil.bytesToObj(_bytes);
        } catch (Exception e) {
            throw new RuntimeException("Unable to deserialize", e);
        }
    }

    @Override
    public ByteBuffer serializeToBuffer(Object obj) {
        try {
            return ByteBuffer.wrap(ObjectUtil.objToBytes(obj));
        } catch (IOException ioe) {
            throw new RuntimeException("Unable to serialize", ioe);
        }
    }

    @Override
    public void serializeToBuffer(Object obj, ByteBuffer buffer) {
        try {
            buffer.put(ObjectUtil.objToBytes(obj));
        } catch (IOException ioe) {
            throw new RuntimeException("Unable to serialize", ioe);
        }
    }

    @Override
    public int estimateSerializedSize(Object obj) {
        return 0;
    }

	@Override
	public Object emptyObject() {
		return new Object();
	}
}
