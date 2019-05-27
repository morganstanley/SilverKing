package com.ms.silverking.thread.lwt.test;

import com.ms.silverking.thread.lwt.LWTPoolProvider;

public class SleepTest {
	private static Sleeper[] createWorkers(int numWorkers) {
		Sleeper[]	workers;
		
		workers = new Sleeper[numWorkers];
		for (int i = 0; i < numWorkers; i++) {
			workers[i] = new Sleeper();
		}
		return workers;
	}
	
	public static void runTest(int numWorkers, int n) {
		Sleeper[]	workers;
		
		workers = createWorkers(numWorkers);
		workers[0].addWork(new Integer(n));
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int	numWorkers;
		int	n;
		
		if (args.length != 2) {
			System.out.println("args: <numWorkers> <n>");
			return;
		}
		LWTPoolProvider.createDefaultWorkPools();
		numWorkers = Integer.parseInt(args[0]);
		n = Integer.parseInt(args[1]);
		runTest(numWorkers, n);
	}
}
