package com.ms.silverking.thread.lwt.test;

import java.util.concurrent.Semaphore;

import com.ms.silverking.thread.lwt.BaseWorker;

public class RingWorker extends BaseWorker<Integer> {
	private final int	id;
	private RingWorker	next;
	private Semaphore	semaphore;
	
	public RingWorker(int id, boolean concurrent, Semaphore semaphore) {
		super(concurrent, RingTest.maxDirectCallDepth);
		this.id = id;
		this.semaphore = semaphore;
	}
	
	public void setNext(RingWorker next) {
		this.next = next;
	}

	@Override
	public void doWork(Integer item) {
		//int	intValue;
		
		//System.out.println(Thread.currentThread());
		//System.out.println("doWork "+ id +" "+ item);
		//intValue = item.intValue();
		if (item == 0) {
			semaphore.release();
			System.out.println(id);
		} else {
			next.addWork(new Integer(item - 1));
		}
	}
}
