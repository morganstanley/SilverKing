package com.ms.silverking.cloud.dht;

/**
 * Protocol used when storing and retrieving data.
 */
public enum ConsistencyProtocol {
    /** 
     * A consistency protocol that favors availability over consistency. Returns success if even a single replica
     * is able to store the value.
     */
    LOOSE, 
    /**
     * A consistency protocol that favors consistency over availability. Only returns success if all replicas
     * are able to store the value.
     */
    TWO_PHASE_COMMIT
}
