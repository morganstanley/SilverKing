package com.ms.silverking.cloud.dht.client;

/**
 * Implemented by classes that listen for callbacks on operation completion or update.
 */
public interface AsyncOperationListener {
	/**
	 *  Issued when an asynchronous operation has updated in a 
	 *  significant way. If INCOMPLETE is *not* specified in listenStates 
	 *  when registering this callback, then this method will be called 
	 *  exactly once for any complete operation. If INCOMPLETE is specified 
	 *  in the listenStates when registering this callback, then this method 
	 *  may be called multiple times for this operation for any given OperationState.
	 * @param asyncOperation the operation that has updated
	 */
	public void asyncOperationUpdated(AsyncOperation asyncOperation);
}
