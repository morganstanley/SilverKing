package com.ms.silverking.thread.lwt.test;

import com.ms.silverking.thread.lwt.BaseWorker;

public class LoadSource extends BaseWorker<Integer> {
	private final int		id;
	private final LoadSink	sink;
	private final int		sinkLoadSize;
	
	public LoadSource(int id, LoadSink sink, int sinkLoadSize, boolean concurrent) {
		super(concurrent);
		this.id = id;
		this.sink = sink;
		this.sinkLoadSize = sinkLoadSize;
	}
	
	@Override
	public int hashCode() {
		return id;
	}
	
	@Override
	public void doWork(Integer item) {
		if (LoadTest.verbose) {
			System.out.println("Source "+ id +" generating load "+ sinkLoadSize);
		}
		for (int i = 0; i < sinkLoadSize; i++) {
			sink.addWork(i);
		}
		if (LoadTest.verbose) {
			System.out.println("Source "+ id +" done generating load ");
		}
	}
}
