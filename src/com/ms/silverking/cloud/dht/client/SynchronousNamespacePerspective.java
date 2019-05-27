package com.ms.silverking.cloud.dht.client;

import java.util.Map;

/**
 * Read/write interface to the DHT. Synchronous - all methods block until they are complete. 
 * 
 * @param <K> key type
 * @param <V> value type
 */
public interface SynchronousNamespacePerspective<K,V> 
					extends SynchronousWritableNamespacePerspective<K, V>,
							SynchronousReadableNamespacePerspective<K, V>{		
    /**
     * Create a view of this namespace perspective as a java.util.Map. 
     * Operations supported are: containsKey(), get(), put(), and putAll(). 
     * @return Map view of this namespace perspective.
     */
    public Map<K,V> asMap();
}
