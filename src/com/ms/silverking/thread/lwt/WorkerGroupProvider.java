package com.ms.silverking.thread.lwt;

/**
 * Provides concrete WorkerGroup implementations
 */
public class WorkerGroupProvider {
	public static <I> WorkerGroup<I> createWorkerGroup(String name, 
										int maxDirectCallDepth, 
										int idleThreadThreshold) {
		return new WorkerGroupImpl<I>(name, 
									  maxDirectCallDepth, 
									  idleThreadThreshold);
	}
	
	public static <I> WorkerGroup<I> createWorkerGroup() {
		return createWorkerGroup(null, 
								 LWTConstants.defaultMaxDirectCallDepth, 
								 LWTConstants.defaultIdleThreadThreshold);
	}
}
