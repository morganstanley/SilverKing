package com.ms.silverking.util;

import java.util.TimerTask;

import com.ms.silverking.log.Log;

/**
 * Wraps TimerTask instances to ensure that all executions do not break the timer due to a thrown exception
 */
public class SafeTimerTask extends TimerTask {
	private final TimerTask	task;
	
	public SafeTimerTask(TimerTask task) {
		this.task = task;
	}
	
	public void run() {
		try {
			task.run();
		} catch (Exception e) {
			Log.logErrorWarning(e, "SafeTimerTask caught exception");
		}
	}
}