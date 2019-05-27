package com.ms.silverking.thread.lwt.test;

import java.util.concurrent.Semaphore;

import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class RingTest {
    static final int    maxDirectCallDepth = 1000;
    
	private static RingWorker[] createWorkers(int numWorkers, boolean concurrent, Semaphore semaphore) {
		RingWorker[]	workers;
		
		workers = new RingWorker[numWorkers];
		for (int i = 0; i < numWorkers; i++) {
			workers[i] = new RingWorker(i, concurrent, semaphore);
			if (i > 0) {
				workers[i - 1].setNext(workers[i]);
			}
		}
		workers[numWorkers - 1].setNext(workers[0]);
		return workers;
	}
	
	public static void runTest(int numWorkers, int n, boolean concurrent) {
		RingWorker[]	workers;
		Semaphore		semaphore;
		Stopwatch		creationSW;
		Stopwatch		sw;
		
		semaphore = new Semaphore(0); 
		creationSW = new SimpleStopwatch();
		workers = createWorkers(numWorkers, concurrent, semaphore);
		creationSW.stop();
		System.out.println("Elapsed creation: "+ creationSW);
		sw = new SimpleStopwatch();
		workers[0].addWork(new Integer(n));
		try {
			semaphore.acquire();
		} catch (InterruptedException ie) {
		}
		sw.stop();
		System.out.println("Elapsed thread ring: "+ sw);
		System.exit(0);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int		numWorkers;
		int		n;
		boolean	concurrent;
		
		if (args.length != 3) {
			System.out.println("args: <numWorkers> <n> <concurrent>");
			System.out.println("standard thread ring is 503 1000 <concurrent>");
			return;
		}
		numWorkers = Integer.parseInt(args[0]);
		n = Integer.parseInt(args[1]);
		concurrent = Boolean.parseBoolean(args[2]);
		LWTPoolProvider.createDefaultWorkPools();
		runTest(numWorkers, n, concurrent);
	}
}
