package com.ms.silverking.cloud.meta;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.zookeeper.CancelableObserver;
import com.ms.silverking.collection.LightLinkedBlockingQueue;
import com.ms.silverking.log.Log;
import com.ms.silverking.process.SafeThread;
import com.ms.silverking.util.SafeTimer;

public abstract class WatcherBase implements Watcher, CancelableObserver {
    protected final MetaClientCore    metaClientCore;
    protected final String    basePath;
    protected volatile boolean    active;
    private final WatcherTimerTask   timerTask;
    private final Lock              lock;
    private final long				minIntervalMillis;
    private final long				intervalMillis;
    private final AtomicLong		lastCheckMillis;
    
    private static final Timer  _timer;
    
    private static final String timerName = "WatcherBaseTimer";
    
    private static final int    minSleepMS = 500;
    private static final int    minIntervalReductionMillis = 100;
    
    private static final LightLinkedBlockingQueue<EventAndWatcher>	watchedEventQueue;
    private static final int	watchedEventQueueTimeoutSeconds = 10;
    private static final int	processRunnerThreads = 6;
    
    
    static {
        _timer = new SafeTimer(timerName, true);
        watchedEventQueue = new LightLinkedBlockingQueue<>();
        new ProcessRunner();
    }
    
    public WatcherBase(MetaClientCore metaClientCore, Timer timer, String basePath, long intervalMillis, long maxInitialSleep) {
    	this.metaClientCore = metaClientCore;
        lock = new ReentrantLock();
        if (timer == null) {
            timer = _timer;
        }
        active = true;
        this.basePath = basePath;
    	this.intervalMillis = intervalMillis;
    	this.minIntervalMillis = Math.max(intervalMillis - minIntervalReductionMillis, 0);
        lastCheckMillis = new AtomicLong();
        timerTask = new WatcherTimerTask();
        timer.schedule(timerTask, ThreadLocalRandom.current().nextInt(Math.max(minSleepMS, (int)maxInitialSleep + 1)), 
                       intervalMillis);
    }
    
    public WatcherBase(MetaClientCore metaClientCore, String basePath, long intervalMillis, long maxInitialSleep) {
        this(metaClientCore, _timer, basePath, intervalMillis, maxInitialSleep);
    }
    
    public void stop() {
        lock.lock();
        try {
            active = false;
            timerTask.cancel();
        } finally {
            lock.unlock();
        }
    }
    
    public boolean isActive() {
    	return active;
    }
    
    protected abstract void _doCheck() throws KeeperException;
    
    protected void doCheck(long curTimeMillis) throws KeeperException {
    	if (active) {
	    	lastCheckMillis.set(curTimeMillis);
	    	_doCheck();
    	}
    }
    
    protected void doCheck() throws KeeperException {
    	if (active) {
    		doCheck(SystemTimeUtil.systemTimeSource.absTimeMillis());
    	}
    }
    
    public void timerRang() throws KeeperException {
    	long	curTimeMillis;
    	
    	curTimeMillis = SystemTimeUtil.systemTimeSource.absTimeMillis();
    	if (curTimeMillis - lastCheckMillis.get() < minIntervalMillis) {
    		Log.fine("Ignoring doCheck() as last call was < intervalMillis");
    	} else {
        	doCheck(curTimeMillis);
    	}
    }

    class WatcherTimerTask extends TimerTask {
        WatcherTimerTask() {
        }
        
        @Override
        public void run() {
            lock.lock();
            try {
                if (active) {
                    timerRang();
                }
            } catch (Exception e) {
                Log.logErrorWarning(e);
            } finally {
                lock.unlock();
            }
        }
    }
    
    public void process(WatchedEvent event) {
    	if (active) {
	    	try {
				watchedEventQueue.put(new EventAndWatcher(event, this));
			} catch (InterruptedException e) {
				Log.logErrorWarning(e);
			}
    	}
    }
    
    static class ProcessRunner implements Runnable {
    	ProcessRunner() {
    		for (int i = 0; i < processRunnerThreads; i++) {
    			new SafeThread(this, "WB.ProcessRunner."+ i, true).start();
    		}
    	}
    	
    	public void run() {
    		while (true) {
	    		try {
	    			EventAndWatcher	ew;
	    			
	    			ew = watchedEventQueue.poll(watchedEventQueueTimeoutSeconds, TimeUnit.SECONDS);
	    			if (ew != null) {
	    				ew.watcherBase.processSafely(ew.event);
	    			} else {
	    				// FUTURE - size a pool, or use lwt
	    			}
	    		} catch (Exception e) {
	    			e.printStackTrace();
	    		}
    		}
    	}
    }
    
    static class EventAndWatcher {
    	final WatchedEvent	event;
    	final WatcherBase	watcherBase;
    	
    	EventAndWatcher(WatchedEvent event, WatcherBase watcherBase) {
    		this.event = event;
    		this.watcherBase = watcherBase;
    	}
    }
    
    public void processSafely(WatchedEvent event) {
    	Log.fine(event);
    	if (active) {
	    	switch (event.getType()) {
	    	case None:
	    		if (event.getState() == KeeperState.SyncConnected) {
	    			Log.fine("Connected");
	    			connected(event);
	    		}
	    		break;
	    	case NodeCreated:
				nodeCreated(event);
	    		break;
	    	case NodeDeleted:
				nodeDeleted(event);
	    		break;
	    	case NodeDataChanged:
				nodeDataChanged(event);
	    		break;
	    	case NodeChildrenChanged:
				nodeChildrenChanged(event);
	    		break;
	    	default:
	    		Log.warning("Unknown event type: ", event.getType());
	    	}
	    } else {
	    	Log.fine("Ignoring. Not active.");
	    }
    }
    
    public void connected(WatchedEvent event) {
    	Log.fine("Ignoring connected");
    }
    
    public void nodeCreated(WatchedEvent event) {
    	Log.fine("Ignoring nodeCreated");
    }
    
    public void nodeDeleted(WatchedEvent event) {
    	Log.fine("Ignoring nodeDeleted");
    }
    
    public void nodeDataChanged(WatchedEvent event) {
    	Log.fine("Ignoring nodeDataChanged");
    }
    
    public void nodeChildrenChanged(WatchedEvent event) {
    	Log.fine("Ignoring nodeChildrenChanged");
    }
}
