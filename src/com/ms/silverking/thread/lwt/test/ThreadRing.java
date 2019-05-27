package com.ms.silverking.thread.lwt.test;

import com.ms.silverking.thread.lwt.BaseWorker;
import com.ms.silverking.thread.lwt.LWTPoolProvider;

public class ThreadRing extends BaseWorker<Integer> {
	private int			id;
	private ThreadRing	next;
	
	public ThreadRing(int id) {
		this.id = id;
	}
	
	public void setNext(ThreadRing next) {
		this.next = next;
	}

	@Override
	public void doWork(Integer count) {
		if (count == 0) {
			System.out.println(id);
			System.exit(0);
		} else {
			next.addWork(count - 1);
		}
	}
	
	public static void main(String[] args) {
		LWTPoolProvider.createDefaultWorkPools();
		
		ThreadRing[] workers = new ThreadRing[503];
		for (int i = 0; i < workers.length; i++) {
			workers[i] = new ThreadRing(i);
			if (i > 0) {
				workers[i - 1].setNext(workers[i]);
			}
		}
		workers[workers.length - 1].setNext(workers[0]);
		
		workers[0].addWork(Integer.parseInt(args[0]));
		try {
		    Thread.sleep(Long.MAX_VALUE);
		} catch (InterruptedException ie) {
		}
	}
}
