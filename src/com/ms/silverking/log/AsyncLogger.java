/**
 * 
 */
package com.ms.silverking.log;

import java.util.concurrent.BlockingQueue;

import com.ms.silverking.thread.ThreadUtil;


class AsyncLogger implements Runnable {
	private final BlockingQueue<LogEntry>	logQueue;
	
	AsyncLogger(BlockingQueue<LogEntry> logQueue) {
		this.logQueue = logQueue;
		ThreadUtil.newDaemonThread(this, "AsyncLogger").start();
	}
	
	public void run() {
		// FUTURE - we can't really shut this one down
		// change implementation if we ever want to do that
		while (true) {
			LogEntry	entry;
			
			try {
				entry = logQueue.take();
				entry.log();
			} catch (InterruptedException ie) {
			}
		}
	}
}