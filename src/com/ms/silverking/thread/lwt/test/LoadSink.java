package com.ms.silverking.thread.lwt.test;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import com.ms.silverking.thread.lwt.BaseWorker;

public class LoadSink extends BaseWorker<Integer> {
	private final int		id;
	private final Semaphore	semaphore;
	private final int		sink;
	private AtomicInteger	numComplete;
	
	public LoadSink(int id, Semaphore semaphore, int loadSize, boolean concurrent) {
		super(concurrent);
		this.id = id;
		this.semaphore = semaphore;
		this.sink = loadSize;
		numComplete = new AtomicInteger();
	}
	
	@Override
	public int hashCode() {
		return id;
	}
	
	@Override
	public void doWork(Integer item) {
		if (LoadTest.verbose) {
			System.out.println("Sink "+ id +" numComplete"+ (numComplete.get() + 1));
		}
		if (numComplete.incrementAndGet() >= sink) {
			if (LoadTest.verbose) {
				System.out.println("Sink "+ id +" complete");
			}
			semaphore.release();
		}
	}
}
