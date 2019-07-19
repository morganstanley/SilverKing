package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.common.DHTKey;

/**
 * Converts user keys into internal DHTKeys.
 */
public interface KeyCreator<K> {
    /**
     * Convert the user key into its DHTKey form
     * @param key
     * @return DHTKey form of key
     */
    public DHTKey createKey(K key);
    public DHTKey[] createSubKeys(DHTKey key, int numSubKeys);
}
