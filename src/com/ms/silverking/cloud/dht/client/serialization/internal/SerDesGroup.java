package com.ms.silverking.cloud.dht.client.serialization.internal;

import com.ms.silverking.cloud.dht.client.serialization.BufferDestSerializer;
import com.ms.silverking.cloud.dht.client.serialization.BufferSourceDeserializer;

/**
 * Groups serializers and deserializers for ease-of-use.
 *
 * Currently unused
 * FUTURE - consider removing this class
 *
 */
public final class SerDesGroup<K,V> {
	//private final ArrayDestSerializer<K>		keySerializer;
	private final BufferSourceDeserializer<K>	keyDeserializer;
	private final BufferDestSerializer<V>		valueSerializer;
	private final BufferSourceDeserializer<V>	valueDeserializer;
	
	public SerDesGroup(//ArrayDestSerializer<K> keySerializer,
						BufferSourceDeserializer<K> keyDeserializer,
						BufferDestSerializer<V> valueSerializer,
						BufferSourceDeserializer<V> valueDeserializer) {
		//this.keySerializer = keySerializer;
		this.keyDeserializer = keyDeserializer;
		this.valueSerializer = valueSerializer;
		this.valueDeserializer = valueDeserializer;
	}
	
	//public ArrayDestSerializer<K> getKeySerializer() {
	//	return keySerializer;
	//}

	public BufferSourceDeserializer<K> getKeyDeserializer() {
		return keyDeserializer;
	}

	public BufferDestSerializer<V> getValueSerializer() {
		return valueSerializer;
	}

	public BufferSourceDeserializer<V> getValueDeserializer() {
		return valueDeserializer;
	}
}
