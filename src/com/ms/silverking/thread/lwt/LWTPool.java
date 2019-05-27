package com.ms.silverking.thread.lwt;

/**
 * A pool of LWTThreads, work, and workers to perform the work. 
 * Workers use pooled threads to perform their work.
 * 
 * If a common work queue is used by workers, it will be common to no more than the
 * LWTPool in which they reside.
 *
 */
public interface LWTPool {
    public LWTPoolLoadStats getLoad();
	public String getName();
	public void dumpStatsOnShutdown();
	public void debug();
}
