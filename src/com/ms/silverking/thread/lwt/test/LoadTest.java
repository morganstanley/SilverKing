package com.ms.silverking.thread.lwt.test;

import java.util.concurrent.Semaphore;

import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.thread.lwt.LWTThreadUtil;
import com.ms.silverking.thread.lwt.WorkerGroup;
import com.ms.silverking.thread.lwt.WorkerGroupProvider;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class LoadTest {
	private final WorkerGroup<Integer>	sourceGroup;
	private final LoadSource[]	sources;
	private final LoadSink[]	sinks;
	private final Semaphore		semaphore;
	private final int			sourceLoadSize;
	
	public static final boolean	verbose = false;
	
	public LoadTest(int numWorkers, boolean concurrent, int sourceLoadSize, int sinkLoadSize) {
		this.sourceLoadSize = sourceLoadSize;
		semaphore = new Semaphore(-numWorkers + 1);
		sources = new LoadSource[numWorkers];
		sinks = new LoadSink[numWorkers];
        sourceGroup = WorkerGroupProvider.createWorkerGroup();
		for (int i = 0; i < numWorkers; i++) {
			sinks[i] = new LoadSink(i, semaphore, sourceLoadSize * sinkLoadSize, concurrent);
			sources[i] = new LoadSource(i, sinks[i], sinkLoadSize, concurrent);
			sourceGroup.addWorker(sources[i]);
		}
	}
	
	public void runTest() {
		Stopwatch	sw;
		
		System.out.println("Running test");
		System.out.println(LWTThreadUtil.isLWTThread());
		sw = new SimpleStopwatch();
		for (int i = 0; i < sourceLoadSize; i++) {
			sourceGroup.broadcastWork(i);
		}
		try {
			semaphore.acquire();
		} catch (InterruptedException ie) {
		}
		sw.stop();
		System.out.println("Test complete: "+ sw);
		System.exit(0);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		LoadTest	loadTest;
		int			numWorkers;
		int			sourceLoadSize;
		int			sinkLoadSize;
		boolean		concurrent;
		
		//Log.setLevelAll();
		if (args.length != 4) {
			System.out.println("args: <numWorkers> <sourceLoadSize> <sinkLoadSize> <concurrent>");
			return;
		}
		numWorkers = Integer.parseInt(args[0]);
		sourceLoadSize = Integer.parseInt(args[1]);
		sinkLoadSize = Integer.parseInt(args[2]);
		concurrent = Boolean.parseBoolean(args[3]);
		
		LWTPoolProvider.createDefaultWorkPools();
		loadTest = new LoadTest(numWorkers, concurrent, sourceLoadSize, sinkLoadSize);
		loadTest.runTest();
	}
}
