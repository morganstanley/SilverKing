package com.ms.silverking.cloud.ring;

import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class TreeMapTest {
	private final int	numNodes;
	private final TreeMap<Long, Integer>	ring;
	
	private static final int	batchSize = 1000000;
	
	public TreeMapTest(int numNodes) {
		this.numNodes = numNodes;
		ring = createRing(numNodes);
	}
	
	private static TreeMap<Long,Integer> createRing(int numNodes) {
		TreeMap<Long,Integer>	ring;
		Random	random;
		
		random = new Random();
		ring = new TreeMap<Long,Integer>();
		for (int i = 0; i < numNodes; i++) {
			ring.put(random.nextLong(), i);
		}
		return ring;
	}
	
	public void runTest(double durationSeconds) {
		Stopwatch	sw;
		Random		random;
		double		secondsPerAccess;
		double		usPerAccess;
		long		totalAccesses;
		
		totalAccesses = 0;
		random = new Random();
		sw = new SimpleStopwatch();
		while (sw.getSplitSeconds() < durationSeconds) {
			for (int i = 0; i < batchSize; i++) {
				long	key;
				Map.Entry<Long, Integer>	entry;
				
				key = random.nextLong();
				entry = ring.ceilingEntry(key);
			}
			totalAccesses += batchSize;
		}
		sw.stop();
		secondsPerAccess = sw.getElapsedSeconds() / (double)totalAccesses;
		usPerAccess = secondsPerAccess * 1e6;
		System.out.println("usPerAccess:\t"+ usPerAccess);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("<numNodes> <duration>");
		} else {
			int			numNodes;
			double		durationSeconds;
			TreeMapTest	treeMapTest;
			
			numNodes = Integer.parseInt(args[0]);
			durationSeconds = Double.parseDouble(args[1]);
			treeMapTest = new TreeMapTest(numNodes);
			treeMapTest.runTest(durationSeconds);
		}
	}
}
