package com.ms.silverking.cloud.dht;

/**
 * Specifies the response of a WaitFor operation to a timeout. Exit quietly or throw an exception.
 */
public enum TimeoutResponse {
    /** Throw an exception when a WaitFor timeout occurs */
    EXCEPTION, 
    /** Ignore the timeout and exit quietly when a WaitFor timeout occurs */
    IGNORE;
    
    /**
     * By default, throw an exception when a timeout occurs.
     */
    public static final TimeoutResponse   defaultResponse = EXCEPTION; 
}
