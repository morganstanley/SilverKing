package com.ms.silverking.cloud.dht.client.serialization;

import java.nio.ByteBuffer;

/**
 * Serializes objects of a particular type to bytes stored in a ByteBuffer.
 *
 * @param <T> type to serialize
 */
public interface BufferDestSerializer<T> {
	public ByteBuffer serializeToBuffer(T obj);
    public void serializeToBuffer(T obj, ByteBuffer buffer);
    public int estimateSerializedSize(T obj);
	public T emptyObject();
}
