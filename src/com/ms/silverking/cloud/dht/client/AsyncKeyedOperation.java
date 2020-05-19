package com.ms.silverking.cloud.dht.client;

import java.util.Map;
import java.util.Set;

/**
 * An asynchronous keyed operation. Provides OperationState for all keys.
 *
 * @param <K> key type
 */
public interface AsyncKeyedOperation<K> extends AsyncOperation {
  /**
   * Get the number of keys in this operation.
   *
   * @return the number of keys in this operation
   */
  public int getNumKeys();

  /**
   * Get the set of keys in this operation.
   *
   * @return the set of keys in this operation
   */
  public Set<K> getKeys();

  /**
   * Get the set of all incomplete keys in this operation.
   *
   * @return the set of keys in this operation
   */
  public Set<K> getIncompleteKeys();

  /**
   * Get the OperationState for the given key.
   *
   * @param key given key
   * @return OperationState for the given key
   */
  public OperationState getOperationState(K key);

  /**
   * Get the OperationState for all keys in this operation
   *
   * @return the OperationState for all keys in this operation
   */
  public Map<K, OperationState> getOperationStateMap();
}
