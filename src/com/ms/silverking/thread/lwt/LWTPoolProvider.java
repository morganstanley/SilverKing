package com.ms.silverking.thread.lwt;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Provides:
 *  a) The capability to create LWTPools.
 *  b) Default work pools that are used by BaseWorker if no other pool is provided.
 */
public class LWTPoolProvider {
	public static LWTPool	defaultNonConcurrentWorkPool;
	public static LWTPool	defaultConcurrentWorkPool;
	
	public static final double	defaultMaxThreadFactor = 20.0;
	
	private static final Object	synch = new Object();
			
    //////////////////////
    // Default work pools
	
	/**
	 * Create the default LWTPools that will be used when no custom pool is provided to BaseWorker.
	 * @param params
	 */
	public static void createDefaultWorkPools(DefaultWorkPoolParameters params) {
		boolean	_created;
		
		_created = created.getAndSet(true);
		if (!_created) {
			synchronized (synch) {
				if (params.getNumNonConcurrentThreads() > 0) {
					if (defaultNonConcurrentWorkPool != null && !params.getIgnoreDoubleInit()) {
						throw new RuntimeException("Double initialization");
					} else {
					    if (defaultNonConcurrentWorkPool == null) {
					        defaultNonConcurrentWorkPool = createPool( LWTPoolParameters.create("defaultNonConcurrent").
						                                    targetSize(params.getNumNonConcurrentThreads()).
						                                    maxSize(params.getMaxConcurrentThreads()).
						                                    commonQueue(false).workUnit(params.getWorkUnit()));
					    }
					}
				}
				if (params.getNumConcurrentThreads() > 0) {
					if (defaultConcurrentWorkPool != null && !params.getIgnoreDoubleInit()) {
						throw new RuntimeException("Double initialization");
					} else {
	                    if (defaultConcurrentWorkPool == null) {
	                        defaultConcurrentWorkPool = createPool( LWTPoolParameters.create("defaultConcurrent").
	                                                    targetSize(params.getNumConcurrentThreads()).
	                                                    maxSize(params.getMaxConcurrentThreads()).
	                                                    commonQueue(true).workUnit(params.getWorkUnit()));
	                    }
					}
				}
			}
		}
	}
	
	private static AtomicBoolean	created = new AtomicBoolean();
	
	/**
	 * Create the default LWTPools using the default parameters.
	 */
	public static void createDefaultWorkPools() {
		createDefaultWorkPools(DefaultWorkPoolParameters.defaultParameters());
	}
	
	//////////////////////
	// Custom work pools
	
	/**
	 * Create a custom LWTPool
	 * @param lwtPoolParameters
	 * @return
	 */
	public static LWTPool createPool(LWTPoolParameters lwtPoolParameters) {
	    return new LWTPoolImpl(lwtPoolParameters);
	}
}
