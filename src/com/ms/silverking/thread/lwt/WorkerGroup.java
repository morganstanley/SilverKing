package com.ms.silverking.thread.lwt;

import java.util.Collection;

/**
 * Collection of workers that accepts group work operations such as
 * broadcasting a work item to all members of the group, scattering
 * a collection of work items among the group, etc.
 *
 * @param <I>
 */
public interface WorkerGroup<I> {
	public String getName();
	public void addWorker(BaseWorker<I> worker);
	public void broadcastWork(I item);
	public void scatterWork(I[] items);
	public void scatterWork(Collection<I> items);
	public void addWork(I item);
}
