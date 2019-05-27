package com.ms.silverking.thread.lwt;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import com.ms.silverking.collection.LightLinkedBlockingQueue;
import com.ms.silverking.log.Log;

/**
 * Concrete LWTPool implementation
 */
class LWTPoolImpl implements LWTPool {
	private final String	name;
	private final int		targetSize;
	private final int		maxSize;
	private final AtomicInteger		blockedThreads;	// 
	private final List<LWTThread>	idleThreads;	// 
	private final List<LWTThread>	activeThreads;
	private final AtomicInteger	nextThread;
	private final Lock				lock;
	private final LightLinkedBlockingQueue<AssignedWork>	commonQueue;
	//private final TransferQueue<AssignedWork>   commonQueue;
	private final LWTPoolController				controller;
	private final int  workUnit;
    private final LWTPoolLoadStats  loadStats;
	private boolean  dumpStatsOnShutdown;
	
	//private static int	maxTotalThreads = LWTConstants.numProcessors * 10;

	private static final long	spinsBeforeParking;
	private static final long	defaultSpinsBeforeParking = 1000000;
	private static final String	spinsBeforeParkingProperty = LWTConstants.propertyBase +".LWTThreadPool.SpinsBeforeParking";
	
	public static final int	defaultPriority = 0;
	
	static {
		String	val;
		
		val = System.getProperty(spinsBeforeParkingProperty);
		if (val != null) {
			spinsBeforeParking = Long.parseLong(val);
		} else {
			spinsBeforeParking = defaultSpinsBeforeParking;
		}
		if (Log.levelMet(Level.INFO)) {
			Log.info(spinsBeforeParkingProperty +": "+ spinsBeforeParking);
		}
	}
	
	/*
	public static void setMaxThreadFactor(double maxThreadFactor) {
		maxTotalThreads = Math.max((int)(LWTConstants.numProcessors * maxThreadFactor), LWTConstants.numProcessors);
		Log.warning("LWTThreadPool maxTotalThreads: ", maxTotalThreads);
	}
	*/
	
	/**
	 * Create a new thread pool.
	 * @param targetSize target number of non-blocked threads.
	 * @param maxSize 
	 */
	public LWTPoolImpl(LWTPoolParameters lwtPoolParameters) {
		this.name = lwtPoolParameters.getName();
		//System.out.println("targetSize: "+ targetSize);
		this.targetSize = lwtPoolParameters.getTargetSize();
		this.maxSize = lwtPoolParameters.getMaxSize();
		if (LWTConstants.verbose) {
			Log.warning("Creating LWTPool: ", lwtPoolParameters);
		    Log.warning(String.format("%s targetSize %d maxSize %d", name, targetSize, maxSize));
		}
        loadStats = new LWTPoolLoadStats();
		// FUTURE - Just use common queue for now since it's faster so far. See addWork().
		if (true || lwtPoolParameters.getCommonQueue()) {
			//this.commonQueue = new BoundedPriorityBlockingQueue<AssignedWork>(1000);
			this.commonQueue = new LightLinkedBlockingQueue<AssignedWork>(spinsBeforeParking);
            //this.commonQueue = new SpinningTransferQueue<AssignedWork>(spinsBeforeParking);
            //this.commonQueue = new SpinningTransferQueue<AssignedWork>();
		} else {
			this.commonQueue = null;
		}
		this.workUnit = lwtPoolParameters.getWorkUnit();
		this.blockedThreads = new AtomicInteger();
		this.idleThreads = new ArrayList<LWTThread>();
		this.activeThreads = new ArrayList<LWTThread>();
		for (int i = 0; i < targetSize; i++) {
			addThread();
		}
		nextThread = new AtomicInteger();
		lock = new ReentrantLock();
		controller = new LWTPoolController(name);
		controller.addPool(this);
	}
	
	@Override
	public String getName() {
		return name;
	}
	
	private void addThread() {
        //TransferQueue<AssignedWork> q;
        //SpinningTransferQueue<AssignedWork> q;
		LightLinkedBlockingQueue<AssignedWork> q;
		LWTThread	thread;
		
		if (LWTConstants.enableLogging) {
			Log.fine("addThread()");
		}
		if (commonQueue != null) {
			q = commonQueue;
		} else {
			q = new LightLinkedBlockingQueue<AssignedWork>(spinsBeforeParking);
            //q = new SpinningTransferQueue<AssignedWork>(spinsBeforeParking);
            //q = new SpinningTransferQueue<AssignedWork>();
		}
		thread = new LWTThread(name +"."+ activeThreads.size(), q, this, workUnit);
		activeThreads.add(thread);
		thread.start();
		//System.out.println("Added: "+ thread);
	}
	
	private void removeThread(int index) {
		LWTThread	thread;
		
		thread = activeThreads.remove(index);
		thread.lwtStop();
		//System.out.println("Stopped: "+ thread);
	}
	
	private void deactivateThread() {
		LWTThread	thread;
		
		thread = activeThreads.remove(activeThreads.size() - 1);
		idleThreads.add(thread);
		thread.setIdle();
	}
	
	private void activateThread() {
		LWTThread	thread;
		
		if (LWTConstants.enableLogging) {
			Log.fine("activateThread()");
		}
		thread = idleThreads.remove(idleThreads.size() - 1);
		activeThreads.add(thread);
		thread.setActive();
	}
	
	private int numActiveThreads() {
		if (LWTConstants.enableLogging) {
			Log.fine("blockedThreads:\t", blockedThreads.get());
		}
		return activeThreads.size() - blockedThreads.get();
	}
	
	private int numTotalThreads() {
		return activeThreads.size() + idleThreads.size();
	}
			
	private void increaseThreadsToLimit() {
		lock.lock();
		try {
			while (numActiveThreads() < targetSize) {
				if (idleThreads.size() == 0) {
					if (numTotalThreads() < maxSize) {
						addThread();
					} else {
						break;
					}
				} else {
					activateThread();
				}
			}
		} finally {
			lock.unlock();
		}
	}
	
	private void decreaseThreadsToLimit() {
		lock.lock();
		try {
			while (numActiveThreads() > targetSize) {
				//removeThread(activeThreads.size() - 1); // FIXME
				deactivateThread();
			}
		} finally {
			lock.unlock();
		}
	}
	
	public void setBlocked(LWTThread thread) {
		blockedThreads.incrementAndGet();
		controller.check(this);
	}
	
	public void setNonBlocked(LWTThread thread) {
		blockedThreads.decrementAndGet();
		controller.check(this);
	}
	
	public void checkThreadLevel() {
	    int    active;
	    
		if (LWTConstants.enableLogging && Log.levelMet(Level.FINE)) {
			Log.warning("numActiveThreads():\t", numActiveThreads());
            Log.warning("numIdleThreads():\t", numIdleThreads());
            Log.warning("numTotalThreads():\t", numTotalThreads());
			Log.warning("targetSize:\t", targetSize);
            Log.warning("maxSize:\t", maxSize);
		}
		active = numActiveThreads();
		loadStats.addLoadSample(active);
		if (active < targetSize) {
			increaseThreadsToLimit();
		} else if (active > targetSize) {
			decreaseThreadsToLimit();
		}
	}
	
	public int numIdleThreads() {
		return idleThreads.size();
	}
	
	/**
	 * Add work to be done and the worker that is to perform this work to 
	 * this pool. 
	 * 
	 * Direct call optimizations must have been performed prior to this
	 * call as this call always results in queueing.
	 * 
	 * If a common queue is in place, simply add. Otherwise, make an 
	 * attempt to find an idle thread, or - failing that - a thread 
	 * with a small queue.
	 * 
	 * @param worker
	 * @param item
	 */
	public void addWork(BaseWorker worker, Object item) {
		addWork(worker, item, 0);
	}
	
	/**
	 * Add work to be done and the worker that is to perform this work to 
	 * this pool. 
	 * 
	 * Direct call optimizations must have been performed prior to this
	 * call as this call always results in queueing.
	 * 
	 * If a common queue is in place, simply add. Otherwise, make an 
	 * attempt to find an idle thread, or - failing that - a thread 
	 * with a small queue.
	 * 
	 * @param worker
	 * @param item
	 */
	public void addWork(BaseWorker worker, Object item, int priority) {
		if (commonQueue != null) {
			try {
				commonQueue.put(new AssignedWork(worker, item, priority));
			} catch (InterruptedException ie) {
			}
		} else {		

            LWTThread   thread;
            int         index;
            
            index = Math.abs(nextThread.getAndIncrement() % activeThreads.size());
            thread = activeThreads.get(index);
            //thread.addWork(worker, item);
		    
			//throw new RuntimeException("deprecated");
			// we would need to make thread list threadsafe
			// if we wanted to get this
			/*
			int		minQueueLength;
			int		minIndex;
			int		startIndex;
			int		index;
			boolean	added;
			
			// Concurrent needs work to win over
			// a shared queue
			
			// Heuristic to try to select a good worker.
			// FUTURE - think about making this have stronger
			// guarantees about picking a good queue; it would
			// need to outweigh the cost of providing the 
			// guarantee.
			minIndex = -1;
			minQueueLength = Integer.MAX_VALUE;
			startIndex = Math.abs(nextThread.get() % targetSize);
			added = false;
			do {
				LWTThread	thread;
				
				index = Math.abs(nextThread.getAndIncrement() % threads.size());
				thread = threads.get(index);
				if (thread.isIdle()) {
					thread.addWork(worker, item);
					added = true;
					break;
				} else {
					int			queueLength;
					
					queueLength = thread.queueLength();
					if (queueLength < minQueueLength) {
						minQueueLength = queueLength;
						minIndex = index;
					}
				}
			} while (index != startIndex);
			if (!added) {
				threads.get(minIndex).addWork(worker, item);
			}
		*/
		}
	}
	
	private void dumpStats(List<LWTThread> threads) {
	    int    maxWorkUnit;
	    long[] workUnitStats;
	    
	    maxWorkUnit = Integer.MIN_VALUE;
        for (LWTThread thread : threads) {
            if (thread.getWorkUnit() > maxWorkUnit) {
                maxWorkUnit = thread.getWorkUnit();
            }
        }
        if (maxWorkUnit > 0) {
            workUnitStats = new long[maxWorkUnit + 1];
            for (LWTThread thread : threads) {
                thread.gatherStats(workUnitStats);
            }
            dumpWorkUnitStats(workUnitStats, "LWTPool work unit stats: "+ name);
        }
	}
	
	static void dumpWorkUnitStats(long[] workUnitStats, String label) {
        long    cumulative;
        
        System.err.println(label);
        cumulative = 0;
        for (int i = 0; i < workUnitStats.length; i++) {
            cumulative += workUnitStats[i] * i;
            System.err.printf("%d\t%d\t%d\t%d\n", i, workUnitStats[i], workUnitStats[i] * i, cumulative);                
        }
	}
	
	private void dumpStats() {
        System.err.println(getName() +"\tdumpStats");
        System.err.println("\nactiveThreads");
        dumpStats(activeThreads);
        System.err.println("\nidleThreads");
        dumpStats(idleThreads);
	}

    @Override
    public void dumpStatsOnShutdown() {
        synchronized (this) {
            if (!dumpStatsOnShutdown) {
                dumpStatsOnShutdown = true;
                Runtime.getRuntime().addShutdownHook(new StatDumper());
            }
        }
    }
    
    private class StatDumper extends Thread {
        public void run() {
            dumpStats();
        }
    }
    
    @Override
    public void debug() {
        System.out.println(name +":"+ commonQueue.size());
    }

    @Override
    public LWTPoolLoadStats getLoad() {
        return loadStats;
    }
}
