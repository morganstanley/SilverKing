package com.ms.silverking.cloud.dht.client;



/**
 * <p>Bundles values and metadata retrieved from a DHT. Will contain 
 * a value and/or metadata depending on what form of retrieve
 * was used to obtain the instance.</p>
 * 
 * @param <V> value type
 */
public interface StoredValueBase<V> {
    /**
     * Get the MetaData for this stored value if it exists
     * @return the MetaData for this stored value if it exists. null if no meta data was retrieved.
     */
	public MetaData getMetaData();
    /**
     * Get the value for this stored value if it exists
     * @return the value for this stored value if it exists. null if no value was retrieved.
     */
	public V getValue();
}
