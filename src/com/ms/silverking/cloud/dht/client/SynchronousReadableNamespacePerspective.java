package com.ms.silverking.cloud.dht.client;

import java.util.Map;
import java.util.Set;

import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.WaitOptions;


/**
 * <p>A namespace perspective that provides synchronous read operations. All operations block until completion.</p>
 * 
 * <p>Retrieval operations are either "Get" operations that return whether or not a desired key has a value associated
 * with it, or "WaitFor" operations that wait in a specified manner for a key-value pair to become present.
 * If all requested keys have a value associated with them, Get and WaitFor are equivalent.</p>
 * 
 * <p>Default RetrievalOptions, GetOptions, and WaitOptions are specified in the NamespacePerspectiveOptions.</p>
 * 
 * @param <K> key type
 * @param <V> value type
 */
public interface SynchronousReadableNamespacePerspective<K,V> extends BaseNamespacePerspective<K,V> {
    /**
     * Base retrieval operation; Gets and WaitFors are just convenience versions of this routine. 
     * @param keys a set of keys to retrieve
     * @param retrievalOptions options for the retrieval
     * @return a Map of keys to StoredValues for all requested keys that were found to have values associated with them
     * @throws RetrievalException TODO
     */
    public Map<K, ? extends StoredValue<V>> retrieve(Set<? extends K> keys, RetrievalOptions retrievalOptions) throws RetrievalException;
    /**
     * Single value retrieval operation. 
     * @param key key to retrieve
     * @param retrievalOptions options for the retrieval
     * @return a StoredValue if a the key was found to have an associated value. null otherwise unless the retrieval
     * options are set to throw an exception for non-existence
     * @throws RetrievalException TODO
     */
    public StoredValue<V> retrieve(K key, RetrievalOptions retrievalOptions) throws RetrievalException;
    
    // get - do not wait for key-value pairs to exist
    /**
     * Multiple-value Get operation.
     * @param keys a set of keys to retrieve
     * @param getOptions options for the Get operation
     * @return a Map of keys to StoredValues for all requested keys that were found to have values associated with them
     * @throws RetrievalException TODO
     */
    public Map<K, ? extends StoredValue<V>> get(Set<? extends K> keys, GetOptions getOptions) throws RetrievalException;
    /**
     * Multiple-value Get operation using default GetOptions. For retrieving values only; not StoredValues.
     * @param keys a set of keys to retrieve
     * @return a Map of keys to values for all requested keys that were found to have values associated with them
     * @throws RetrievalException TODO
     */
    public Map<K, V> get(Set<? extends K> keys) throws RetrievalException;
    /**
     * A single-value Get operation.
     * @param key key to retrieve
     * @param getOptions options for the Get operation
     * @return the StoredValue associated with the key if it exists. null otherwise unless the retrieval options
     * are set to throw and exception for non-existence.
     * @throws RetrievalException TODO
     */
    public StoredValue<V> get(K key, GetOptions getOptions) throws RetrievalException;
    /**
     * Single-value Get operation using default GetOptions. For retrieving values only; not StoredValues.
     * @param key key to retrieve
     * @return value associated with the key if it exists. null otherwise unless the default GetOptions
     * are set to throw and exception for non-existence.
     * @throws RetrievalException TODO
     */
    public V get(K key) throws RetrievalException;
        
    // waitFor - wait on non-existent key-value pairs
    /**
     * Multi-value WaitFor operation. 
     * @param keys a set of keys to retrieve
     * @param waitOptions options for the WaitFor operation
     * @return a Map of keys to StoredValues for all requested keys that were found to have values associated with them
     * @throws RetrievalException TODO
     */
    public Map<K, ? extends StoredValue<V>> waitFor(Set<? extends K> keys, WaitOptions waitOptions) throws RetrievalException;
    /**
     * Multi-value WaitFor operation using default WaitFor operations. For retrieving values only; not StoredValues.
     * @param keys a set of keys to retrieve
     * @return a Map of keys to values for all requested keys that were found to have values associated with them
     * @throws RetrievalException TODO
     */
    public Map<K, V> waitFor(Set<? extends K> keys) throws RetrievalException;
    /**
     * Single-value WaitFor operation.
     * @param key key to retrieve
     * @param waitOptions options for the WaitFor operation
     * @return StoredValue associated with the key if it exists. null otherwise unless the default WaitOptions
     * are set to throw and exception for non-existence.
     * @throws RetrievalException TODO
     */
    public StoredValue<V> waitFor(K key, WaitOptions waitOptions) throws RetrievalException;
    /**
     * Single-value WaitFor operation using default WaitOptions. For retrieving values only; not StoredValues.
     * @param key key to retrieve
     * @return value associated with the key if it exists. null otherwise unless the default WaitOptions
     * are set to throw and exception for non-existence.
     * @throws RetrievalException TODO
     */
    public V waitFor(K key) throws RetrievalException;
}
