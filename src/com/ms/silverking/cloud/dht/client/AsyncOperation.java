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
	/**
	 * Adds a completion listener. If the operation is already complete, the callback will be 
	 * immediately executed, possibly in the calling thread.
	 * Equivalent to addListener(listener, OperationState.SUCCEEDED, OperationState.FAILED)
	 * This operation will notify this listener at most once.
	 * @param listener	completion listener
	 */
	public void addListener(AsyncOperationListener listener);
	/**
	 * Adds an operation listener. If the operation is already complete, the callback will be 
	 * immediately executed, possibly in the calling thread.
	 * If OperationState.INCOMPLETE is in listenStates, the listener may be notified multiple times,
	 * and the listener must be prepared for this. If INCOMPLETE is not specified, then 
	 * this operation will notify the listener at most once.
	 * @param listener	update listener
	 * @param listenStates	states to generate updates for
	 */
	public void addListener(AsyncOperationListener listener, OperationState... listenStates);
	/**
	 * Adds multiple completion listeners. For any listener that is already complete, the callback will be 
	 * immediately executed, possibly in the calling thread.
	 * Equivalent to addListeners(listeners, OperationState.SUCCEEDED, OperationState.FAILED)
	 * This operation will notify this listener at most once.
	 * @param listeners	update listeners
	 */
	public void addListeners(Iterable<AsyncOperationListener> listeners);
	/**
	 * Adds multiple completion listeners. For any listener that is already complete, the callback will be 
	 * immediately executed, possibly in the calling thread.
	 * If OperationState.INCOMPLETE is in listenStates, the listener may be notified multiple times,
	 * and the listener must be prepared for this. If INCOMPLETE is not specified, then 
	 * this operation will notify the listener at most once.
	 * @param listeners	update listeners
	 * @param listenStates	states to generate updates for
	 */
	public void addListeners(Iterable<AsyncOperationListener> listeners, OperationState... listenStates);
}
