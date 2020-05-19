package com.ms.silverking.cloud.dht.client.serialization;

import java.nio.ByteBuffer;

/**
 * Deserializes objects from bytes in a ByteBuffer for a particular type
 *
 * @param <T> type to deserialize
 */
public interface BufferSourceDeserializer<T> {
  public T deserialize(ByteBuffer[] bytes);

  public T deserialize(ByteBuffer bytes);
}
