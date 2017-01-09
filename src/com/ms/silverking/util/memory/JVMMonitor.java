package com.ms.silverking.util.memory;

import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ms.silverking.log.Log;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

/**
 * Monitor JVM memory.
 */
public class JVMMonitor implements Runnable {
	private boolean			running;
	
	private final int		minUpdateIntervalMillis;
	private final int		maxUpdateIntervalMillis;
	private final int		finalizationIntervalMillis;
	private final double	absChangeTriggerMB = 10.0;
	private final double	relChangeTrigger = 0.05;
	private final Runtime	runtime;
	private final boolean	display;
	private final Stopwatch	finalizationSW;
	private final Stopwatch	sw;
	private final AtomicBoolean	cleanupHinted;
	private final List<JVMMemoryObserver>	memoryObservers;
	
	private boolean	deltaTriggered;
	
	private final double	lowMemoryThresholdMB;
	private boolean	memoryLow;
		
	private long	freeMemory;
	private long	maxMemory;
	private long	totalMemory;
	
	private double	freeMemoryMB;
	private double	maxMemoryMB;
	private double	totalMemoryMB;
	
	public JVMMonitor(int minUpdateIntervalMillis, int maxUpdateIntervalMillis, 
					int finalizationIntervalMillis, boolean display,
					double lowMemoryThresholdMB) {
		this.minUpdateIntervalMillis = minUpdateIntervalMillis;
		this.maxUpdateIntervalMillis = maxUpdateIntervalMillis;
		this.finalizationIntervalMillis = finalizationIntervalMillis;
		this.display = display;
		if (lowMemoryThresholdMB < 0.0) {
			throw new RuntimeException("Bad lowMemoryThreshold: "+ lowMemoryThresholdMB);
		}
		this.lowMemoryThresholdMB = lowMemoryThresholdMB;
		memoryLow = false;
		runtime = Runtime.getRuntime();
		running = true;
		cleanupHinted = new AtomicBoolean(false);
		finalizationSW = new SimpleStopwatch();
		sw = new SimpleStopwatch();
		memoryObservers = new Vector<JVMMemoryObserver>();
		new Thread(this, "JVMMonitor").start();
	}
	
	public JVMMonitor(int minUpdateIntervalMillis, int maxUpdateIntervalMillis, 
			int finalizationIntervalMillis, boolean display) {
		this(minUpdateIntervalMillis, maxUpdateIntervalMillis,
				finalizationIntervalMillis, display, 0);
	}
	
	public void addMemoryObserver(JVMMemoryObserver memoryObserver) {
		memoryObservers.add(memoryObserver);
	}
	
	public boolean memoryLow() {
		return memoryLow;
	}
	
	public void cleanupHint() {
		cleanupHinted.set(true);
	}
	
	private void notifyMemoryObservers(boolean memoryIsLow) {
		for (JVMMemoryObserver memoryObserver : memoryObservers) {
			memoryObserver.jvmMemoryLow(memoryIsLow);
		}
	}
	
	private void notifyMemoryObservers(long bytesFree) {
		for (JVMMemoryObserver memoryObserver : memoryObservers) {
			memoryObserver.jvmMemoryStatus(bytesFree);
		}
	}
	
	public void monitor() {
		boolean	currentMemoryLow;
		
		if (finalizationSW.getSplitMillis() > finalizationIntervalMillis || cleanupHinted.get()) {
			finalizationSW.reset();
			Log.warning("Forcing finalization");
			System.runFinalization();
			System.gc();
			cleanupHinted.set(false);
		}
		
		freeMemory = runtime.freeMemory();
		maxMemory = runtime.maxMemory();
		totalMemory = runtime.totalMemory();

		triggerCheck(freeMemoryMB, bytesToMB(freeMemory));
		triggerCheck(maxMemoryMB, bytesToMB(maxMemory));
		triggerCheck(totalMemoryMB, bytesToMB(totalMemory));
		
		freeMemoryMB = bytesToMB(freeMemory);
		maxMemoryMB = bytesToMB(maxMemory);
		totalMemoryMB = bytesToMB(totalMemory);
		
		if (freeMemoryMB < lowMemoryThresholdMB) {
			System.gc();
			freeMemoryMB = bytesToMB(freeMemory);
			maxMemoryMB = bytesToMB(maxMemory);
			totalMemoryMB = bytesToMB(totalMemory);
			currentMemoryLow = freeMemoryMB < lowMemoryThresholdMB;
			if (currentMemoryLow) {
				Log.warning("Memory is low: "+ freeMemoryMB);
			}
		} else {
			currentMemoryLow = false;
		}
		notifyMemoryObservers(freeMemory);
		if (currentMemoryLow != memoryLow) {
			memoryLow = currentMemoryLow;
			notifyMemoryObservers(currentMemoryLow);
		}
	}
	
	private void triggerCheck(double ov, double nv) {
		if (Math.abs(ov - nv) > absChangeTriggerMB) {
			deltaTriggered = true;
		}
		if (Math.abs(1.0 - ov / nv) > relChangeTrigger) {
			deltaTriggered = true;
		}
	}
	
	final double bytesToMB(long bytes) {
		double	mb;
		
		mb = (double)bytes / (1024.0 * 1024.0);
		return mb;
	}
	
	public void displayStatus() {
		Log.warning(statusString());
	}
	
	public String statusString() {
	    return String.format("JVMMonitor: %f\t%4.2f\t%4.2f\t%4.2f\t%s", 
	            sw.getSplitSeconds(), freeMemoryMB, maxMemoryMB, totalMemoryMB, memoryLow);
	}
	
	public void run() {
		Stopwatch	displaySW;
		
		ThreadUtil.randomSleep(minUpdateIntervalMillis);
		finalizationSW.reset();
		displaySW = new SimpleStopwatch();
		while (running) {
			try {
				monitor();
				if (display) {
					boolean	displayTriggered;
					
					displayTriggered = false;
					if (displaySW.getSplitMillis() > maxUpdateIntervalMillis) {
						displayTriggered = true;
					}
					if (deltaTriggered) {
						displayTriggered = true;
					}
					if (displayTriggered) {
						deltaTriggered = false;
						displaySW.reset();
						displayStatus();
					}
				}
				ThreadUtil.sleep(minUpdateIntervalMillis);
			} catch (Exception e) { 
				Log.logErrorSevere(e, "JVMMonitor", "run");
				ThreadUtil.pauseAfterException();
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new JVMMonitor(1000, 20 * 1000, 20 * 1000, true);
	}

}
