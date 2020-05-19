package com.ms.silverking.cloud.dht.client;

/**
 * An asynchronous value retrieval for a single key. Supports raw value access.
 */
public interface AsyncSingleValueRetrieval<K, V> extends AsyncValueRetrieval<K, V>, AsyncSingleRetrieval<K, V> {
  /**
   * Returns the raw value if it is present.
   *
   * @return the raw value if it is present
   * @throws RetrievalException TODO
   */
  public V getValue() throws RetrievalException;
}
