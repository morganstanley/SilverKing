package com.ms.silverking.cloud.dht.client;


/**
 * Enumeration of failure causes. 
 */
public enum FailureCause {
	ERROR, TIMEOUT, MUTATION, MULTIPLE, INVALID_VERSION, SIMULTANEOUS_PUT, NO_SUCH_VALUE, NO_SUCH_NAMESPACE, CORRUPT;
}
