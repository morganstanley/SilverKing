package com.ms.silverking.cloud.dht.client;

import java.util.Map;

import com.ms.silverking.cloud.dht.RetrievalOptions;

/**
 * An asynchronous retrieval. May be polled for partial results. Results are
 * returned as StoredValues.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface AsyncRetrieval<K, V> extends AsyncKeyedOperation<K> {
  /**
   * RetrievalOptions used in this operation
   *
   * @return the RetrievalOptions used in this operation
   */
  public RetrievalOptions getRetrievalOptions();

  /**
   * Block until this operation is complete.
   *
   * @throws RetrievalException TODO
   */
  public void waitForCompletion() throws RetrievalException;

  /**
   * Returns StoredValues for all successfully complete retrievals.
   *
   * @return StoredValues for all successfully complete retrievals
   * @throws RetrievalException TODO
   */
  public Map<K, ? extends StoredValue<V>> getStoredValues() throws RetrievalException;

  /**
   * Returns StoredValues for the given key if it is present.
   *
   * @param key key to query
   * @return StoredValues for the given key if it is present
   * @throws RetrievalException TODO
   */
  public StoredValue<V> getStoredValue(K key) throws RetrievalException;

  /**
   * Returns StoredValues for all successfully complete retrievals that
   * have completed since the last call to this method or AsyncValueRetrieval.getLatestStoredValues().
   * Each successfully retrieved value will be reported exactly once by this
   * method and AsyncValueRetrieval.getLatestStoredValues().
   * This method is unaffected by calls to getStoredValues().
   * Concurrent execution is permitted, but the precise values returned are
   * undefined.
   *
   * @return StoredValues for all successfully complete retrievals that
   * have completed since the last call to this method or AsyncValueRetrieval.getLatestStoredValues().
   * @throws RetrievalException TODO
   */
  public Map<K, ? extends StoredValue<V>> getLatestStoredValues() throws RetrievalException;
}
