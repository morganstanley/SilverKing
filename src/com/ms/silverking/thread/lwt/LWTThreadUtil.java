package com.ms.silverking.thread.lwt;


/**
 * Provides LWT-related thread utilities.
 *
 */
public class LWTThreadUtil {
	/**
	 * Treat the current Java thread as an LWT thread.
	 */
	public static void setLWTThread() {
		ThreadState.setLWTThread();
	}
	
	/**
	 * Determine if this is an LWT thread
	 * @return true if this is an LWT thread
	 */
	public static boolean isLWTThread() {
		return ThreadState.isLWTThread();
	}
	
	/**
	 * Set the current LWT thread as blocked. Ignore if the
	 * current thread is not an LWT thread.
	 */
	public static void setBlocked() {
		if (ThreadState.isLWTCompatibleThread()) {
			((LWTCompatibleThread)Thread.currentThread()).setBlocked();
		}
	}
	
	/**
	 * Set the current LWT thread as non-blocked. Ignore if the
	 * current thread is not an LWT thread.
	 */
	public static void setNonBlocked() {
		if (ThreadState.isLWTCompatibleThread()) {
			((LWTCompatibleThread)Thread.currentThread()).setNonBlocked();
		}
	}
}
