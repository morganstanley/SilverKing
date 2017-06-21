package com.ms.silverking.cloud.dht.client;

import java.util.Set;

import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.WaitOptions;

/**
 * <p>A namespace perspective that provides asynchronous read operations. All operations return immediately, 
 * and provide an object that can be used to query the state of the asynchronous operation.</p>
 * 
 * <p>Retrieval operations are either "Get" operations that complete whether or not a desired key has a 
 * value associated with it, or "WaitFor" operations that wait in a specified manner for a key-value pair 
 * to become present.
 * If all requested keys have a value associated with them, Get and WaitFor are equivalent.</p>
 * 
 * <p>Default RetrievalOptions, GetOptions, and WaitOptions are specified in the NamespacePerspectiveOptions.</p>
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface AsynchronousReadableNamespacePerspective<K,V> extends BaseNamespacePerspective<K,V> {
	/**
     * Base retrieval operation; Gets and WaitFors are just convenience versions of this routine. 
     * @param keys a set of keys to retrieve
     * @param retrievalOptions options for the retrieval
     * @return an AsyncRetrieval object representing the operation
	 * @throws RetrievalException
	 */
	public AsyncRetrieval<K,V> retrieve(Set<? extends K> keys, RetrievalOptions retrievalOptions) throws RetrievalException;
	
	
	// get - do not wait for key-value pairs to exist
	// multi-value
	/**
     * Multiple-value Get operation.
     * @param keys a set of keys to retrieve
     * @param getOptions options for the Get operation
     * @return an AsyncRetrieval object representing the operation
	 * @throws RetrievalException
	 */
	public AsyncRetrieval<K,V> get(Set<? extends K> keys, GetOptions getOptions) throws RetrievalException;
	/**
     * Multiple-value Get operation using default GetOptions. For retrieving values only; not StoredValues.
     * @param keys a set of keys to retrieve
     * @return an AsyncValueRetrieval object representing the operation
	 * @throws RetrievalException
	 */
	public AsyncValueRetrieval<K,V> get(Set<? extends K> keys) throws RetrievalException;
	// single-value
	/**
     * A single-value Get operation.
     * @param key key to retrieve
     * @param getOptions options for the Get operation
     * @return an AsyncRetrieval object representing the operation
	 * @throws RetrievalException
	 */
	public AsyncRetrieval<K,V> get(K key, GetOptions getOptions) throws RetrievalException;
	/**
     * Single-value Get operation using default GetOptions. For retrieving values only; not StoredValues.
     * @param key key to retrieve
     * @return an AsyncSingleValueRetrieval object representing the operation
	 * @throws RetrievalException
	 */
	public AsyncSingleValueRetrieval<K,V> get(K key) throws RetrievalException;
		
	
	// waitFor - wait on non-existent key-value pairs
	// single-value
	/**
     * Multi-value WaitFor operation. 
     * @param keys a set of keys to retrieve
     * @param waitOptions options for the WaitFor operation
     * @return an AsyncRetrieval object representing the operation
	 * @throws RetrievalException
	 */
	public AsyncRetrieval<K,V> waitFor(Set<? extends K> keys, WaitOptions waitOptions) throws RetrievalException;
	/**
     * Multi-value WaitFor operation using default WaitFor operations. For retrieving values only; not StoredValues.
     * @param keys a set of keys to retrieve
     * @return an AsyncValueRetrieval object representing the operation
	 * @throws RetrievalException
	 */
	public AsyncValueRetrieval<K,V> waitFor(Set<? extends K> keys) throws RetrievalException;
	// multi-value
	/**
     * Single-value WaitFor operation.
     * @param key key to retrieve
     * @param waitOptions options for the WaitFor operation
     * @return an AsyncRetrieval object representing the operation
	 * @throws RetrievalException
	 */
	public AsyncRetrieval<K,V> waitFor(K key, WaitOptions waitOptions) throws RetrievalException;
	/**
     * Single-value WaitFor operation using default WaitOptions. For retrieving values only; not StoredValues.
     * @param key key to retrieve
     * @return an AsyncSingleValueRetrieval object representing the operation
	 * @throws RetrievalException
	 */
	public AsyncSingleValueRetrieval<K,V> waitFor(K key) throws RetrievalException;
}
