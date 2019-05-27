package com.ms.silverking.thread.lwt;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ms.silverking.log.Log;
import com.ms.silverking.thread.ThreadUtil;

/**
 * Watches LWTThreadPools and adjusts the number of threads in each pool.
 */
class LWTPoolController implements Runnable {
	private final List<LWTPoolImpl>	pools;
	private boolean	running;
	private Lock		lock;
	private Condition	cv;
	
	private static final int	checkIntervalMillis = 1000;
	private static final boolean   debugPool = false;
	
	LWTPoolController(String name) {
		if (LWTConstants.enableLogging) {
			Log.fine("LWTPoolController()");
		}
		pools = new CopyOnWriteArrayList<>();
		running = true;
		lock = new ReentrantLock();
		cv = lock.newCondition();
		ThreadUtil.newDaemonThread(this, "LWTPoolController."+ name).start();
		Thread.setDefaultUncaughtExceptionHandler(new LWTUncaughtExceptionHandler());
	}
	
	public void check(LWTPoolImpl pool) {
		lock.lock();
		try {
			cv.signalAll();
		} finally {
			lock.unlock();
		}
	}
	
	public void addPool(LWTPoolImpl pool) {
		pools.add(pool);
	}
	
	private void checkPool(LWTPoolImpl pool) {
		pool.checkThreadLevel();
		if (debugPool) {
		    pool.debug();
		}
	}
	
	private void checkAllPools() {
		if (LWTConstants.enableLogging) {
			Log.fine("LWTPoolController.checkAllPools()");
		}
		for (LWTPoolImpl pool : pools) {
			checkPool(pool);
		}
	}
	
	public void run() {
		if (LWTConstants.enableLogging) {
			Log.fine("LWTPoolController.run()");
		}
		while (running) {
			try {
				lock.lock();
				try {
					cv.await(checkIntervalMillis, TimeUnit.MILLISECONDS);
				} finally {
					lock.unlock();
				}
				checkAllPools();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
