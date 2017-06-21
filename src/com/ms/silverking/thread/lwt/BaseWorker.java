package com.ms.silverking.thread.lwt;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import com.ms.silverking.log.Log;
import com.ms.silverking.thread.ThreadUtil;

/**
 * Nearly-complete implementation of Worker. Users extend and 
 * fill in the doWork() method to make it complete.
 *
 * @param <I> type of work objects
 */
public abstract class BaseWorker<I> {
	private final boolean		allowsConcurrentWork;
	private final LWTPoolImpl	threadPool;
	private final int			workerMaxDirectCallDepth;
	private final int			idleThreadThreshold;
		
	public BaseWorker(LWTPool workPool, boolean allowsConcurrentWork, 
					  int workerMaxDirectCallDepth, int idleThreadThreshold) {
	    assert workPool != null;
		if (workPool == null) {
			if (LWTPoolProvider.defaultConcurrentWorkPool == null 
					&& LWTPoolProvider.defaultNonConcurrentWorkPool == null) {
				throw new RuntimeException("WorkPools not created");
			} else {
				if (allowsConcurrentWork) {
					workPool = LWTPoolProvider.defaultConcurrentWorkPool;
				} else {
					workPool = LWTPoolProvider.defaultNonConcurrentWorkPool;
				}
			}
		}
		this.allowsConcurrentWork = allowsConcurrentWork;
		this.threadPool = (LWTPoolImpl)workPool;
		this.workerMaxDirectCallDepth = workerMaxDirectCallDepth;
		this.idleThreadThreshold = idleThreadThreshold;
	}
	
	public BaseWorker(LWTPool workPool, boolean allowsConcurrentWork, 
			  int workerMaxDirectCallDepth) {
		this(workPool, allowsConcurrentWork, workerMaxDirectCallDepth, LWTConstants.defaultIdleThreadThreshold);
	}
	
	public BaseWorker(LWTPool workPool, boolean allowsConcurrentWork) {
		this(workPool, allowsConcurrentWork, LWTConstants.defaultMaxDirectCallDepth, LWTConstants.defaultIdleThreadThreshold);
	}
	
	public BaseWorker(boolean allowsConcurrentWork, int maxDirectCallDepth) {
		this(LWTPoolProvider.defaultConcurrentWorkPool, true, maxDirectCallDepth, LWTConstants.defaultIdleThreadThreshold);
	}
	
	public BaseWorker(boolean allowsConcurrentWork, int maxDirectCallDepth, int idleThreadThreshold) {
		this(LWTPoolProvider.defaultConcurrentWorkPool, true, maxDirectCallDepth, idleThreadThreshold);
	}
	
	public BaseWorker(boolean allowsConcurrentWork) {
		this(allowsConcurrentWork ? LWTPoolProvider.defaultConcurrentWorkPool : LWTPoolProvider.defaultNonConcurrentWorkPool, allowsConcurrentWork);
	}
	
	public BaseWorker() {
		this(true, LWTConstants.defaultMaxDirectCallDepth, LWTConstants.defaultIdleThreadThreshold);
	}
	
	//
	
	public final boolean allowsConcurrentWork() {
		return allowsConcurrentWork;
	}
	
	public final void addWork(I item) {
		addWork(item, Integer.MAX_VALUE, idleThreadThreshold);
	}
	
	public final void addWork(I item, int callerMaxDirectCallDepth) {
        addWork(item, callerMaxDirectCallDepth, idleThreadThreshold);
	}
	
	public final void addWork(I item, int callerMaxDirectCallDepth, 
			int idleThreadThreshold) {
		addPrioritizedWork(item, 0, callerMaxDirectCallDepth, idleThreadThreshold);
	}
	
	public final void addPrioritizedWork(I item, int priority) {
		addPrioritizedWork(item, priority, Integer.MAX_VALUE, idleThreadThreshold);
	}
	
	/**
	 * Fundamental work addition methods. Other methods feed into this method. 
	 * @param item
	 * @param priority
	 * @param callerMaxDirectCallDepth
	 * @param idleThreadThreshold
	 */
	public final void addPrioritizedWork(I item, int priority, 
								int callerMaxDirectCallDepth, 
								int idleThreadThreshold) {
		int	maxDirectCallDepth;
		
		/*
		 * Two cases here: 1 - We queue the work to the pool. 2 - We can bypass the queue and 
		 * directly do the work.
		 * 
		 * We prefer 2, but must use 1 for: non-lwt threads, whenever the call depth exceeds
		 * the limit specified, or whenever too many threads are idle.
		 * The idle thread threshold is used to prevent cores from going idle while a single
		 * core does all of the work.
		 */
		maxDirectCallDepth = Math.min(callerMaxDirectCallDepth, workerMaxDirectCallDepth);
		if (/*!ThreadState.isLWTThread()*/ false // FIXME think about this
				|| threadPool.numIdleThreads() > idleThreadThreshold 
				|| ThreadState.getDepth() >= maxDirectCallDepth
				) {
		    // Queue the work
			if (LWTConstants.enableLogging && Log.levelMet(Level.FINE)) {
				Log.fine("\tqueueing", item);
			}
			threadPool.addWork(this, item);
		} else {
		    // Directly call this work
			if (LWTConstants.enableLogging && Log.levelMet(Level.FINE)) {
				Log.fine("\tdirect call", item);
			}
			try {
				callDoWork(item);
			} catch (RuntimeException re) {
				re.printStackTrace();
				// FUTURE - add option to rethrow
			}
		}
	}
	
    public void callDoWork(I[] items) {
        ThreadState.incrementDepth();
        try {
            doWork(items);
        } finally {
            ThreadState.decrementDepth();
        }
    }
    
	public final void callDoWork(I item) {
		ThreadState.incrementDepth();
		try {
			doWork(item);
		} finally {
			ThreadState.decrementDepth();
		}
	}
	
    public void doWork(I[] items, int startIndex, int endIndex) {
        for (int i = startIndex; i <= endIndex; i++) {
            doWork(items[i]);
        }
    }
    
    public void doWork(I[] items) {
        //System.out.println("BaseWorker.doWork([])");
        for (I item : items) {
            doWork(item);
        }
    }
    
	public abstract void doWork(I item);
	
	@Override
	public String toString() {
		StringBuilder	sb;
		
		sb = new StringBuilder();
		sb.append(super.toString());
		sb.append(':');
		sb.append(allowsConcurrentWork);
		return sb.toString();
	}
	
	//private AtomicBoolean  foo = new AtomicBoolean(false);

    public I[] newWorkArray(int size) {
        //if (foo.compareAndSet(false, true)) {
        //    ThreadUtil.printStackTraces();
        //}
        //throw new RuntimeException(this +" doesn't support multiple work");
        return null;
    }
}
