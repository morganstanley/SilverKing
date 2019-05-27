package com.ms.silverking.cloud.dht.client;

import java.util.Map;
import java.util.Set;

import com.ms.silverking.cloud.dht.InvalidationOptions;
import com.ms.silverking.cloud.dht.PutOptions;

/**
 * A namespace perspective that provides asynchronous write operations.
 * 
 * @param <K> key type
 * @param <V> value type
 */
public interface AsynchronousWritableNamespacePerspective<K, V> extends BaseNamespacePerspective<K,V> {
	/**
     * Multi-value Put operation
     * @param values map of key-value pairs to store
     * @param putOptions options for the Put operation
	 * @return an AsyncPut object representing this operation
	 */
	public AsyncPut<K> put(Map<? extends K, ? extends V> values, PutOptions putOptions);
	/**
     * Multi-value Put operation using default PutOptions.
     * @param values map of key-value pairs to store
     * @return an AsyncPut object representing this operation
	 */
	public AsyncPut<K> put(Map<? extends K, ? extends V> values);
	/**
     * Single-value Put operation.
     * @param key key to associate the value with
     * @param value value to store
     * @param putOptions options for the Put operation
     * @return an AsyncPut object representing this operation
	 */
	public AsyncPut<K> put(K key, V value, PutOptions putOptions);
	/**
     * Single-value Put operation using default PutOptions.
     * @param key key to associate the value with
     * @param value value to store
     * @return an AsyncPut object representing this operation
	 */
	public AsyncPut<K> put(K key, V value);
	
	
	/**
     * Multi-value Invalidation operation
     * @param keys keys to invalidate
     * @param invalidationOptions options for the Invalidation operation
	 * @return an AsyncInvalidation object representing this operation
	 */
	public AsyncInvalidation<K> invalidate(Set<? extends K> keys, InvalidationOptions invalidationOptions);
	/**
     * Multi-value Invalidation operation using default InvalidationOptions.
     * @param keys keys to invalidate
     * @return an AsyncInvalidation object representing this operation
	 */
	public AsyncInvalidation<K> invalidate(Set<? extends K> keys);
	/**
     * Single-value Invalidation operation.
     * @param key key to invalidate
     * @param invalidationOptions options for the Invalidation operation
     * @return an AsyncInvalidation object representing this operation
	 */
	public AsyncInvalidation<K> invalidate(K key, InvalidationOptions invalidationOptions);
	/**
     * Single-value Invalidation operation using default InvalidationOptions.
     * @param key key to invalidate
     * @return an AsyncInvalidation object representing this operation
	 */
	public AsyncInvalidation<K> invalidate(K key);
	
	/*
	public AsyncSnapshot snapshot(long version);
	public AsyncSnapshot snapshot();
	*/
	
    /*
    public AsyncSyncRequest syncRequest(long version);
    public AsyncSyncRequest syncRequest();
    */
}
