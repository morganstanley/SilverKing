package com.ms.silverking.cloud.dht;

/**
 * Specifies the response of a Retrieval operation to a non-existing value 
 */
public enum NonExistenceResponse {
    /** Return a null value for keys without associated values */
    NULL_VALUE,
    /** Throw an exception if a key has no associated value */
    EXCEPTION;
    
    /** By default, non-existence returns null values */
    public static final NonExistenceResponse   defaultResponse = NULL_VALUE; 
}
