package com.ms.silverking.cloud.dht.client.impl;

import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ms.silverking.cloud.dht.NonExistenceResponse;
import com.ms.silverking.cloud.dht.client.AsyncOperation;
import com.ms.silverking.cloud.dht.client.FailureCause;
import com.ms.silverking.cloud.dht.client.OpTimeoutController;
import com.ms.silverking.cloud.dht.client.OperationException;
import com.ms.silverking.cloud.dht.client.OperationState;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoMessageGroup;
import com.ms.silverking.log.Log;

/**
 * AsyncOperationImpl provides a concrete implementation of AsyncOperation
 * and wraps an Operation with current state. 
 */
abstract class AsyncOperationImpl implements AsyncOperation {
	protected final Operation  operation;
    protected byte[]           originator;
    private final Lock         lock;
    private final Condition    cv;
    private volatile OpResult           result;
    protected final EnumSet<OpResult>   allResults;
    
    private static final boolean    spin = true;
    private static final int        spinDurationNanos = 5 * 1000;
    
    // Attempt state
    private volatile boolean   sent;    // a hint as to whether or not this operation has been sent before
                                        // used to optimize the first message creation
    protected OpTimeoutState  timeoutState;
    
	public AsyncOperationImpl(Operation operation, long curTimeMillis, byte[] originator) {
        OpTimeoutController   timeoutController;
        
	    assert operation != null;
	    assert originator != null;
        
		this.operation = operation;
        this.originator = originator;
        lock = new ReentrantLock();
        cv = lock.newCondition();
		result = OpResult.INCOMPLETE;
		allResults = EnumSet.noneOf(OpResult.class);
        // FUTURE - could lazily create the timeout state since most ops never need it
		//System.out.printf("%s %d\n", timeoutParameters.hasRelTimeout(), timeoutParameters.getRelTimeout());
		
		timeoutController = operation.getTimeoutController();
		timeoutState = new OpTimeoutState(this, timeoutController, curTimeMillis);
    }
	
	protected abstract NonExistenceResponse getNonExistenceResponse();
	
	protected final boolean isActive() {
        return !result.isComplete();
	}
	
	boolean opHasTimedOut(long curTimeMillis) {
	    return timeoutState.opHasTimedOut(curTimeMillis);
	}
	
    boolean attemptHasTimedOut(long curTimeMillis) {
        return timeoutState.attemptHasTimedOut(curTimeMillis);
    }
    
    void newAttempt(long curTimeMillis) {
        timeoutState.newAttempt(curTimeMillis);
    }
    
	public ClientOpType getType() {
		return operation.getOpType();
	}
	
	public final OperationUUID getUUID() {
		return operation.getUUID();
	}
	
	@Override
	public OperationState getState() {
		return result.toOperationState(getNonExistenceResponse());
	}
	
	protected OpResult getResult() {
		return result;
	}
	
	@Override
	public FailureCause getFailureCause() {
	    return result.toFailureCause(getNonExistenceResponse());
	}
	
    @Override
    public void close() {
        cleanup();
    }
	
	protected abstract int opWorkItems();
	
    protected boolean isFailure(OpResult result) {
        return result.hasFailed(getNonExistenceResponse());
    }
	
	protected void failureCleanup(FailureCause failureCause) {
	    cleanup();
	}
	
    protected void cleanup() {        
    }
	
    // must hold completionCheckLock
    protected void setResult(EnumSet<OpResult> results) {
        assert results.size() > 0;
        
        allResults.addAll(results);
        if (results.size() > 1) {
            setResult(OpResult.MULTIPLE);
        } else {
            setResult(results.iterator().next());
        }
    }
    
    protected void setResult(OpResult result) {
        assert result.isComplete();
        
        if (this.result.isComplete()) {
            if (result != this.result) {
                Log.warning("AsyncOperationImpl.setResult() ignoring new completion", this);
            } else {
                Log.fine("AsyncOperationImpl.setResult() received duplicate completion", this);
            }
        } else {
            lock.lock();
            try {
                if (!this.result.isComplete()) {
                    //System.out.println("Setting result to: "+ result);
                    if (isFailure(result)) {
                        failureCleanup(result.toFailureCause(getNonExistenceResponse()));
                    }
                    this.result = result;
                    cv.signalAll();
                } else {
                    if (result != this.result) {
                        Log.warning("AsyncOperationImpl.setResult() ignoring new completion", this);
                    } else {
                        Log.fine("AsyncOperationImpl.setResult() received duplicate completion", this);
                    }
                }
            } finally {
                lock.unlock();
            }
            cleanup();
        }
    }       
	
	public boolean waitForCompletion(long timeout, TimeUnit unit) throws OperationException {
		long      relativeDeadlineMillis;
		long      absoluteDeadlineMillis;
		
		relativeDeadlineMillis = TimeUnit.MILLISECONDS.convert(timeout, unit);
		absoluteDeadlineMillis = SystemTimeUtil.systemTimeSource.absTimeMillis() + relativeDeadlineMillis; 
        lock.lock();
		try {
			while (!result.isComplete()) {
				try {
					long	millisToDeadline;
					
					millisToDeadline = absoluteDeadlineMillis - SystemTimeUtil.systemTimeSource.absTimeMillis();
					if (millisToDeadline > 0) {
						Log.fine("activeOp awaiting ", this);
						if (!spin) {
						    cv.await(millisToDeadline, TimeUnit.MILLISECONDS);
						} else {
						    cv.awaitNanos(spinDurationNanos);
						}
					} else {
					    debugTimeout();
						return false;
					}
				} catch (InterruptedException ie) {
				}
			}
			if (isFailure(result)) {
			    throwFailedException();
			}
			return true;
		} finally {
            lock.unlock();
		}
	}
	
	protected void debugTimeout() {	    
	}
	
	protected abstract void throwFailedException() throws OperationException;
	
    void _waitForCompletion() throws OperationException {
        lock.lock();
        try {
            while (!result.isComplete()) {
                try {
                    Log.fine("activeOp awaiting ", this);
                    if (!spin) {
                        cv.await();
                    } else {
                        cv.awaitNanos(spinDurationNanos);
                    }
                } catch (InterruptedException ie) {
                }
            }
            if (isFailure(result)) {
                throwFailedException();
            }
        } finally {
            lock.unlock();
        }
    }
	
	public boolean poll() {
		boolean	complete;
		
		lock.lock();
		try {
			complete = result.isComplete();
		} finally {
            lock.unlock();
		}
		return complete;
	}
	
	public boolean poll(long duration, TimeUnit timeUnit) {
		boolean	complete;
		
        lock.lock();
		try {
			while (!result.isComplete()) {
				try {
				    if (!spin) {
				        cv.await(duration, timeUnit);
				    } else {
                        cv.awaitNanos(spinDurationNanos);
				    }
				} catch (InterruptedException ie) {
				}
			}
			complete = result.isComplete();
		} finally {
            lock.unlock();
		}
		return complete;
	}
	
	protected void setSent() {
	    sent = true;
	}
	
	protected boolean getSent() {
	    return sent;
	}
	
    abstract void addToEstimate(MessageEstimate estimate);
	abstract MessageEstimate createMessageEstimate();
    abstract ProtoMessageGroup createProtoMG(MessageEstimate estimate);
    abstract ProtoMessageGroup createMessagesForIncomplete(ProtoMessageGroup protoMG, List<MessageGroup> messageGroups, MessageEstimate estimate);
	
	@Override
	public String toString() {
		StringBuilder	sb;
		
		sb = new StringBuilder();
		sb.append(super.toString());
		sb.append(operation.getOpType());
		sb.append(':');
		sb.append(getUUID());
		sb.append(':');
		sb.append(result);
		return sb.toString();
	}
	
	// for debugging
	protected final String objectToString() {
	    return super.toString();
	}

	/**
	 * Determine if this operation can be grouped with another operation in a single
	 * message.
	 * @param asyncOperationImpl
	 * @return True iff this operation can be another operation 
	 */
    public boolean canBeGroupedWith(AsyncOperationImpl asyncOperationImpl) {
        return asyncOperationImpl.getType() == getType();
    }
}
