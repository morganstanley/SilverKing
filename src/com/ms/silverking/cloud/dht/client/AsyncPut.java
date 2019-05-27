package com.ms.silverking.cloud.dht.client;

/**
 * An asynchronous Put.
 * @param <K> key type
 */
public interface AsyncPut<K> extends AsyncKeyedOperation<K> {
	/**
	 * Block until this operation is complete.
	 */
	public void waitForCompletion() throws PutException;
}
