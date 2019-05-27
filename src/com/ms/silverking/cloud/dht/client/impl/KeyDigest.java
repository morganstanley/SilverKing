package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.common.DHTKey;

/**
 * Computes a DHTKey from a byte array.
 */
public interface KeyDigest {
    public DHTKey computeKey(byte[] bytes);
    public byte[] getSubKeyBytes(DHTKey key, int subKeyIndex);
    public DHTKey[] createSubKeys(DHTKey key, int numSubKeys);
}
