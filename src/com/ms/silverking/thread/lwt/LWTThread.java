package com.ms.silverking.thread.lwt;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import com.ms.silverking.collection.LightLinkedBlockingQueue;
import com.ms.silverking.log.Log;
import com.ms.silverking.thread.ThreadUtil;

/**
 * A thread specialized to only do LWT work.
 *
 */
class LWTThread extends Thread implements LWTCompatibleThread {
    private final int   workUnit; 
	//private final BlockingQueue<AssignedWork>	q;
    private final LightLinkedBlockingQueue<AssignedWork> q;
	private final LWTPoolImpl	threadPool;
	private boolean		running;
	private boolean		active;
	private Lock		idleLock;
	private Condition	idleCV;
	private int         depth;
	
	private final long[] workUnitStats;
	
	static final int   defaultWorkUnit = 1;
	
	private static final boolean   enableStats = false;  
	
	/**
	 * @param name
	 * @param q
	 * @param idleThreads - must have been initialized by the pool
	 */
	public LWTThread(String name, LightLinkedBlockingQueue<AssignedWork> q, LWTPoolImpl threadPool,
    //public LWTThread(String name, BlockingQueue<AssignedWork> q, LWTPoolImpl threadPool,
	                int workUnit) {
		super(name);
		this.q = q;
		this.threadPool = threadPool;
		this.workUnit = workUnit;
		if (enableStats) {
		    workUnitStats = new long[workUnit + 1];
		} else {
            workUnitStats = null;
		}
		running = true;
		idleLock = new ReentrantLock();
		idleCV = idleLock.newCondition();
		active = true;
		setDaemon(true);
	}
	
	/*
    public LWTThread(String name, LightLinkedBlockingQueue<AssignedWork> q, LWTPoolImpl threadPool) {
        this(name, q, threadPool, defaultWorkUnit);
    }
    */
    
    public int getWorkUnit() {
        return workUnit;
    }
    
	public BlockingQueue<AssignedWork> getQueue() {
	    return q;
	}
	
	public final void incrementDepth() {
	    depth++;
	}
	
	public final void decrementDepth() {
	    depth--;
	}
	
	public final int getDepth() {
	    return depth;
	}
	
	public void lwtStop() {
		running = false;
	}
	
	@Override
	public void setBlocked() {
		threadPool.setBlocked(this);
	}
	
    @Override
	public void setNonBlocked() {
		threadPool.setNonBlocked(this);
	}
	
	public void setActive() {
		if (active) {
			throw new RuntimeException("Double activation");
		}
		active = true;
		idleLock.lock();
		try {
			idleCV.signalAll();
		} finally {
			idleLock.unlock();
		}
	}
	
	public void setIdle() {
		if (!active) {
			throw new RuntimeException("Double inactivation");
		}
		active = false;
	}
	
	public void run() {
	    if (workUnit == 1) {
            runSingle();
	    } else {
            runMultiple();
	    }
	}
	
	private void runSingle() {
		ThreadState.setLWTThread();
		while (running) {
			try {
				while (active) {
					AssignedWork	work;
					
					work = q.take();
					if (Log.levelMet(Level.FINE)) {
						Log.fine(this +" doWork "+ work);
					}
					//System.out.println(idleThreads.get());
					try {
						work.doWork();
		        	} catch (Exception e) {
		                Log.logErrorWarning(e);
		                ThreadUtil.pauseAfterException();        		
		        	}
				}
				while (!active) {
					idleLock.lock();
					try {
						idleCV.await();
					} finally {
						idleLock.unlock();
					}
				}
			} catch (Exception e) {
				Throwable	t;
				
				Log.warning("************************************** "+ getName());
				t = e;
				while (t != null) {
					Log.logErrorWarning(t);
					t = t.getCause();
					Log.warning("......................................");
				}
				ThreadUtil.pauseAfterException();
			}
		}
	}
	
	private void activeMultiple(AssignedWork[] workList) {
        BaseWorker  worker;
        int         numWorkItems;
        
        try {
            numWorkItems = q.takeMultiple(workList);
            /*
            AssignedWork    w;
            
            w = q.take();
            workList[0] = w;
            numWorkItems = 1;
            */
        } catch (InterruptedException ie) {
            throw new RuntimeException("Interruption not supported", ie);
        }
        if (enableStats) {
            ++workUnitStats[numWorkItems];                    
        }
        if (numWorkItems == 1) { // special case to speed up single work items
        	try {
        		workList[0].doWork();
        	} catch (Exception e) {
                Log.logErrorWarning(e);
                ThreadUtil.pauseAfterException();        		
        	}
        } else {
            int         groupStart;
            int         i; // index used to search for first incompatible worker
            
            groupStart = 0;
            while (groupStart < numWorkItems) {
                Object[]    _workList; // work items copied out from workers
                
                worker = workList[groupStart].getWorker();
                // compute the multiple work list
                for (i = groupStart + 1; i < numWorkItems; i++) {
                    if (workList[i].getWorker() != worker) {
                        break;
                    }
                }
                _workList = worker.newWorkArray(i - groupStart);
                if (_workList != null) {
                    for (int j = groupStart; j < i; j++) {
                        _workList[j - groupStart] = workList[j].getWork();
                    }
                    try {
                    	worker.doWork(_workList);
                	} catch (Exception e) {
                        Log.logErrorWarning(e);
                        ThreadUtil.pauseAfterException();        		
                	}
                } else {
                    for (int j = groupStart; j < i; j++) {
                    	try {
	                        workList[j].doWork();
                    	} catch (Exception e) {
                            Log.logErrorWarning(e);
                            ThreadUtil.pauseAfterException();        		
                    	}
                    }
                }
                groupStart = i;
            }
        }
	}
	
    private void runMultiple() {
        AssignedWork[]  workList;
        
        ThreadState.setLWTThread();
        //System.out.println("LWTThread.runMultiple()\t"+ this);
        workList = new AssignedWork[workUnit];
        while (running) {
            try {
                while (active) {
                    activeMultiple(workList);
                    Arrays.fill(workList, null); 
                    // Above null fill is to ensure that complete work is GC'd
                    // The current strategy is designed to reduce the overhead of
                    // this clearing operation.
                    // We could do this one-by-one if needed, but this is
                    // unlikely due to the fact that tasks should be very quick.
                }
                while (!active) {
                    idleLock.lock();
                    try {
                        idleCV.await();
                    } finally {
                        idleLock.unlock();
                    }
                }
            } catch (Exception e) {
                Log.logErrorWarning(e);
                ThreadUtil.pauseAfterException();
            }
        }
    }
	
    public void gatherStats(long[] _workUnitStats) {
        for (int i = 0; i < workUnitStats.length; i++) {
            _workUnitStats[i] += workUnitStats[i];
        }
    }
    
    public void dumpStats() {
        if (enableStats) {
            LWTPoolImpl.dumpWorkUnitStats(workUnitStats, "LWTStats: "+ getName());
        }
    }
}
