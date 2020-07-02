package com.ms.silverking.cloud.dht.client.impl;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.NonExistenceResponse;
import com.ms.silverking.cloud.dht.client.AsyncOperation;
import com.ms.silverking.cloud.dht.client.AsyncOperationListener;
import com.ms.silverking.cloud.dht.client.FailureCause;
import com.ms.silverking.cloud.dht.client.OpTimeoutController;
import com.ms.silverking.cloud.dht.client.OperationException;
import com.ms.silverking.cloud.dht.client.OperationState;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoMessageGroup;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.log.Log;
import com.ms.silverking.thread.lwt.BaseWorker;
import com.ms.silverking.thread.lwt.LWTPool;
import com.ms.silverking.thread.lwt.LWTPoolParameters;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.util.PropertiesHelper;

/**
 * AsyncOperationImpl provides a concrete implementation of AsyncOperation
 * and wraps an Operation with current state.
 */
abstract class AsyncOperationImpl implements AsyncOperation {
  protected final Operation operation;
  protected byte[] originator;
  private final Lock lock;
  private final Condition cv;
  private volatile OpResult result;
  protected final EnumSet<OpResult> allResults;
  private volatile Set<Pair<AsyncOperationListener, EnumSet<OperationState>>> listeners;

  private static final boolean spin = true;
  private static final int spinDurationNanos = 5 * 1000;

  // Attempt state
  private volatile boolean sent;    // a hint as to whether or not this operation has been sent before
  // used to optimize the first message creation
  protected OpTimeoutState timeoutState;

  private static Map<OperationUUID, AsyncOperationImpl> opsWithListeners = new ConcurrentHashMap<>();

  public AsyncOperationImpl(Operation operation, long curTimeMillis, byte[] originator) {
    OpTimeoutController timeoutController;

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

  boolean retryOnExclusionChange(long curTimeMillis) {
    return timeoutState.retryOnExclusionChange(curTimeMillis);
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
      Set<Pair<AsyncOperationListener, EnumSet<OperationState>>> _listeners;
      boolean isCompletion;

      isCompletion = false;
      _listeners = null;
      lock.lock();
      try {
        if (!this.result.isComplete()) {
          //System.out.println("Setting result to: "+ result);
          if (isFailure(result)) {
            failureCleanup(result.toFailureCause(getNonExistenceResponse()));
          }
          this.result = result;
          isCompletion = result.isComplete();
          cv.signalAll();
          if (listeners != null) {
            _listeners = ImmutableSet.copyOf(listeners);
          }
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
      if (_listeners != null) {
        notificationWorker.filterForUpdates(this, _listeners, getState());
        if (isCompletion) {
          opsWithListeners.remove(getUUID());
        }
      }
      cleanup();
    }
  }

  /**
   * Called by subclasses to update incomplete state. Complete updates are handled inside of
   * setResult to ensure that completion results fire exactly once.
   */
  protected void checkForUpdates() {
    if (listeners != null) {
      Set<Pair<AsyncOperationListener, EnumSet<OperationState>>> _listeners;
      OperationState opState;

      opState = getState();
      if (opState == OperationState.INCOMPLETE) {
        _listeners = null;
        lock.lock();
        try {
          if (listeners != null) {
            _listeners = ImmutableSet.copyOf(listeners);
          }
        } finally {
          lock.unlock();
        }
        if (_listeners != null) {
          notificationWorker.filterForUpdates(this, _listeners, opState);
        }
      }
    }
  }

  private void notifyListeners(Iterable<AsyncOperationListener> listeners) {
    for (AsyncOperationListener listener : listeners) {
      listener.asyncOperationUpdated(this);
    }
  }

  private static final String notificationWorkerPoolSizeProperty = AsyncOperationImpl.class.getPackage().getName() +
      ".NotificationWorkerPoolSize";
  private static final String notificationWorkerMaxDirectCallDepthProperty =
      AsyncOperationImpl.class.getPackage().getName() + ".NotificationWorkerMaxDirectCallDepth";
  private static final int notificationWorkerMaxDirectCallDepth;
  private static final LWTPool notificationWorkerPool;
  private static final NotificationWorker notificationWorker;

  static {
    int notificationWorkerPoolSize;

    notificationWorkerPoolSize = PropertiesHelper.systemHelper.getInt(notificationWorkerPoolSizeProperty, 0);
    Log.finef("notificationWorkerPoolSize %d", notificationWorkerPoolSize);
    if (notificationWorkerPoolSize > 0) {
      notificationWorkerPool = LWTPoolProvider.createPool(LWTPoolParameters.create("NotificationWorkerPool").targetSize(
          notificationWorkerPoolSize).maxSize(notificationWorkerPoolSize).workUnit(1));
    } else {
      notificationWorkerPool = LWTPoolProvider.defaultConcurrentWorkPool;
    }
    notificationWorker = new NotificationWorker(notificationWorkerPool);
    notificationWorkerMaxDirectCallDepth = PropertiesHelper.systemHelper.getInt(
        notificationWorkerMaxDirectCallDepthProperty, 0);
    Log.finef("notificationWorkerMaxDirectCallDepth", notificationWorkerMaxDirectCallDepth);
  }

  private static class NotificationWorker extends BaseWorker<Pair<AsyncOperationImpl, Set<AsyncOperationListener>>> {
    NotificationWorker(LWTPool lwtPool) {
      super(lwtPool, true);
    }

    void filterForUpdates(AsyncOperationImpl opImpl,
        Set<Pair<AsyncOperationListener, EnumSet<OperationState>>> _listeners, OperationState opState) {
      Set<AsyncOperationListener> listeners;

      listeners = new HashSet<>();
      for (Pair<AsyncOperationListener, EnumSet<OperationState>> candidate : _listeners) {
        if (candidate.getV2().contains(opState)) {
          listeners.add(candidate.getV1());
        }
      }
      update(opImpl, listeners);
    }

    void update(AsyncOperationImpl opImpl, Set<AsyncOperationListener> listeners) {
      if (listeners.size() > 0) {
        addWork(new Pair<>(opImpl, listeners), notificationWorkerMaxDirectCallDepth);
      }
    }

    @Override
    public void doWork(Pair<AsyncOperationImpl, Set<AsyncOperationListener>> p) {
      p.getV1().notifyListeners(p.getV2());
    }
  }

  public boolean waitForCompletion(long timeout, TimeUnit unit) throws OperationException {
    long relativeDeadlineMillis;
    long absoluteDeadlineMillis;

    relativeDeadlineMillis = TimeUnit.MILLISECONDS.convert(timeout, unit);
    absoluteDeadlineMillis = SystemTimeUtil.skSystemTimeSource.absTimeMillis() + relativeDeadlineMillis;
    lock.lock();
    try {
      while (!result.isComplete()) {
        try {
          long millisToDeadline;

          millisToDeadline = absoluteDeadlineMillis - SystemTimeUtil.skSystemTimeSource.absTimeMillis();
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

  /**
   * Adds a completion listener. If the operation is already complete, the callback will be
   * immediately executed, possibly in the calling thread.
   * Equivalent to addListener(listener, OperationState.SUCCEEDED, OperationState.FAILED)
   *
   * @param listener completion listener
   */
  public void addListener(AsyncOperationListener listener) {
    addListener(listener, OperationState.SUCCEEDED, OperationState.FAILED);
  }

  /**
   * Adds multiple completion listeners. For any listener that is already complete, the callback will be
   * immediately executed, possibly in the calling thread.
   * Equivalent to addListeners(listeners, OperationState.SUCCEEDED, OperationState.FAILED)
   *
   * @param listeners update listeners
   */
  public void addListeners(Iterable<AsyncOperationListener> listeners) {
    addListeners(listeners, OperationState.SUCCEEDED, OperationState.FAILED);
  }

  /**
   * Adds an operation listener. If the operation is already complete, the callback will be
   * immediately executed, possibly in the calling thread.
   * Updates of completion will occur exactly once. Updates of other states may
   * occur multiple times and may occur in any order.
   *
   * @param listener     update listener
   * @param listenStates states to generate updates for
   */
  public void addListener(AsyncOperationListener listener, OperationState... listenStates) {
    addListeners(ImmutableList.of(listener), listenStates);
  }

  /**
   * Adds multiple completion listeners. For any listener that is already complete, the callback will be
   * Adds multiple listeners. If the operation is already complete, the callback will be
   * immediately executed, possibly in the calling thread.
   * Updates of completion will occur exactly once. Updates of other states may
   * occur multiple times and may occur in any order.
   *
   * @param listeners    update listeners
   * @param listenStates states to generate updates for
   */
  public void addListeners(Iterable<AsyncOperationListener> _listeners, OperationState... listenStates) {
    EnumSet<OperationState> _listenStates;
    OperationState opState;
    boolean notifyListeners;

    opState = null;
    notifyListeners = false;
    _listenStates = CollectionUtil.arrayToEnumSet(listenStates);
    lock.lock();
    try {
      // Trigger immediate updates for completion, but only for completion
      opState = getState();
      if (_listenStates.contains(opState)) {
        notifyListeners = true;
      }
      // We must add this to listeners if either:
      //   1 - We are not notifying listeners now
      //   2 - This call requests notification of incomplete state
      //       (This will trigger both immediate callback and subsequent callbacks)
      if (!notifyListeners || _listenStates.contains(OperationState.INCOMPLETE)) {
        if (listeners == null) {
          listeners = new HashSet<>();
        }
        for (AsyncOperationListener listener : _listeners) {
          listeners.add(new Pair<>(listener, _listenStates));
        }
        opsWithListeners.put(getUUID(), this);
      }
    } finally {
      lock.unlock();
    }
    if (notifyListeners) {
      // Using calling thread to trigger updates for completion, but only for completion
      if (opState != OperationState.INCOMPLETE) {
        notifyListeners(_listeners);
      } else {
        notificationWorker.update(this, ImmutableSet.copyOf(_listeners));
      }
    }
  }

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
    boolean complete;

    lock.lock();
    try {
      complete = result.isComplete();
    } finally {
      lock.unlock();
    }
    return complete;
  }

  public boolean poll(long duration, TimeUnit timeUnit) {
    boolean complete;

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

  abstract ProtoMessageGroup createMessagesForIncomplete(ProtoMessageGroup protoMG, List<MessageGroup> messageGroups,
      MessageEstimate estimate);

  @Override
  public String toString() {
    StringBuilder sb;

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
   *
   * @param asyncOperationImpl
   * @return True iff this operation can be another operation
   */
  public boolean canBeGroupedWith(AsyncOperationImpl asyncOperationImpl) {
    return asyncOperationImpl.getType() == getType();
  }
}
