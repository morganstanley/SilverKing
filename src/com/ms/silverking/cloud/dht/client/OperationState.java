package com.ms.silverking.cloud.dht.client;

/**
 * A point-in-time view of the state of an operation. Used both to represent 
 * the state of bulk operations as well as components of bulk operations. A given
 * bulk operation may have FAILED but have individual parts that SUCCEEDED.
 */
public enum OperationState {
	INCOMPLETE, SUCCEEDED, FAILED
}
