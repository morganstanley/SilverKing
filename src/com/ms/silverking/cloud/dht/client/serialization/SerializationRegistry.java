package com.ms.silverking.cloud.dht.client.serialization;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.ms.silverking.log.Log;

/**
 * Registry for all implemented serializers. Allows retrieval by type.
 */
public class SerializationRegistry {
	private final Map<Class, BufferDestSerializer>	serializers;
	private final Map<Class, BufferSourceDeserializer>	deserializers;
	
	private static final boolean   debug = false;
	
    public static SerializationRegistry createEmptyRegistry() {
        return new SerializationRegistry();
    }
    
    public static SerializationRegistry createDefaultRegistry() {
        SerializationRegistry   defaultRegistry;
        
        defaultRegistry = new SerializationRegistry();
        defaultRegistry.addSerDes(byte[].class, new RawByteArraySerDes());
        defaultRegistry.addSerDes(String.class, new StringSerDes());
        defaultRegistry.addSerDes(Object.class, new ObjectSerDes());
        defaultRegistry.addSerDes(Long.class, new LongSerDes());
        defaultRegistry.addSerDes(Integer.class, new IntegerSerDes());
        defaultRegistry.addSerDes(Short.class, new ShortSerDes());
        defaultRegistry.addSerDes(UUID.class, new UUIDSerDes());
        return defaultRegistry;
    }
    
	private SerializationRegistry() {
		serializers = new HashMap<Class, BufferDestSerializer>();
		deserializers = new HashMap<Class, BufferSourceDeserializer>();
	}
	
    public <T> void addSerDes(Class<T> srcClass, BufferSerDes<T> serDes) {
        addSerializer(srcClass, serDes);
        addDeserializer(srcClass, serDes);
    }
    
	public <T> void addSerializer(Class<T> srcClass, BufferDestSerializer<T> serializer) {
	    if (debug) {
	        Log.warning("addSerializer: "+ srcClass +" "+ serializer +"\t"+ this);
	    }
		serializers.put(srcClass, serializer);
	}
	
	public <T> void addDeserializer(Class<T> destClass, BufferSourceDeserializer<T> deserializer) {
        if (debug) {
            Log.warning("addDeserializer: "+ destClass +" "+ deserializer +"\t"+ this);
        }
		deserializers.put(destClass, deserializer);
	}
	
	public <T> BufferDestSerializer<T> getSerializer(Class<T> srcClass) {
        if (debug) {
            Log.warning("getSerializer: "+ srcClass +" "+ serializers.get(srcClass) +"\t"+ this);
        }
		return (BufferDestSerializer<T>)serializers.get(srcClass);
	}
	
	public <T> BufferSourceDeserializer<T> getDeserializer(Class<T> destClass) {
        if (debug) {
            Log.warning("getDeserializer: "+ destClass +" "+ deserializers.get(destClass) +"\t"+ this);
        }
		return (BufferSourceDeserializer<T>)deserializers.get(destClass);
	}
	
    //public <T> BufferDestSerializer<T> getBufferDestSerializer(Class<T> srcClass) {
    //    return (BufferDestSerializer<T>)serializers.get(srcClass);
    //}
}
