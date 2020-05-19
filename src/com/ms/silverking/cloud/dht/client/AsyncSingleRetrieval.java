package com.ms.silverking.cloud.dht.client;

/**
 * An asynchronous retrieval for a single key. Result is returned as a StoredValue.
 */
public interface AsyncSingleRetrieval<K, V> extends AsyncRetrieval<K, V> {
  /**
   * Returns the raw value if it is present.
   *
   * @return the raw value if it is present
   * @throws RetrievalException
   */
  public StoredValue<V> getStoredValue() throws RetrievalException;
}
