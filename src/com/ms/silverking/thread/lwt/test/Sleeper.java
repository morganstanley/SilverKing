package com.ms.silverking.thread.lwt.test;

import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.thread.lwt.BaseWorker;

public class Sleeper extends BaseWorker<Integer> {
	private static final boolean	verbose = true;
	
	@Override
	public void doWork(Integer item) {
		if (verbose) {
			System.out.println("Sleeping: "+ item);
		}
		ThreadUtil.sleepSeconds(item.doubleValue());
		if (verbose) {
			System.out.println("Done sleeping: "+ item);
		}
	}
}
