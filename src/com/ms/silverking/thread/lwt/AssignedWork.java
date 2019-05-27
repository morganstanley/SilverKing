package com.ms.silverking.thread.lwt;

/**
 * Associates work to be done with the worker that will do the work. 
 */
class AssignedWork implements Comparable<AssignedWork> {
	private final BaseWorker	worker;
	private final Object		work;
	private final int			priority;
	
	public AssignedWork(BaseWorker worker, Object work, int priority) {
		this.worker = worker;
		this.work = work;
		this.priority = priority;
	}
	
	public BaseWorker getWorker() {
	    return worker;
	}
	
	public Object getWork() {
		return work;
	}
	
	public void doWork() {
		worker.callDoWork(work);
	}
	
	@Override
	public int compareTo(AssignedWork other) {
		if (priority < other.priority) {
			return -1; 
		} else if (priority > other.priority) {
			return 1;
		} else {
			return 0;
		}
	}
	
	public String toString() {
		StringBuilder	sb;
		
		sb = new StringBuilder();
		sb.append(worker);
		sb.append(':');
		sb.append(work);
		return sb.toString();
	}
}
