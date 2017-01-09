package com.ms.silverking.cloud.dht.client;

import java.util.concurrent.TimeUnit;

/**
 * An asynchronous operation. May be polled for operation status, blocked on, or closed.
 */
public interface AsyncOperation {
	/**
	 * Query operation state.
	 * @return operation state
	 */
	public OperationState getState();
	/**
	 * Query cause of failure. Only valid for operations that have, in fact, failed.
	 * @return underlying FailureCause
	 */
	public FailureCause getFailureCause();
	/**
	 * Block until this operation is complete.
	 */
	public void waitForCompletion() throws OperationException;
	/**
     * Block until this operation is complete. Exit after the given timeout
	 * @param timeout time to wait
	 * @param unit unit of time to wait
	 * @return true if this operation is complete. false otherwise
	 * @throws OperationException
	 */
	public boolean waitForCompletion(long timeout, TimeUnit unit) throws OperationException;
	/**
	 * Close this asynchronous operation. No subsequent calls may be issued against
	 * this reference.
	 */
	public void close();
}
