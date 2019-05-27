package com.ms.silverking.net.async;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import com.ms.silverking.collection.LightLinkedBlockingQueue;
import com.ms.silverking.thread.lwt.LWTThreadUtil;

/**
 * Active send. Allows waiting on the send and returning any error.
 */
public final class ActiveSend /*implements AsyncSendListener*/ {
	private final BlockingQueue<IOException>	queue;
	//private final SynchronousQueue<IOException>	synchronousQueue;
//	private final AsyncSendListener				listener;
	
	private static final IOException	SUCCESS = new IOException();
	private static final int			activeSendOfferTimeoutMillis = 1000;
	
	public ActiveSend(/*AsyncSendListener listener*/) {
		queue = new LightLinkedBlockingQueue<IOException>();
		//synchronousQueue = new SynchronousQueue<IOException>();
		//this.listener = listener;
	}
	
	public void setException(IOException exception) {
		try {
			queue.put(exception);
			//synchronousQueue.offer(exception, activeSendOfferTimeoutMillis, TimeUnit.MILLISECONDS);
		} catch (InterruptedException ie) {
		}
	}
	
	public void setSentSuccessfully() {
		try {
			queue.put(SUCCESS);
			//synchronousQueue.offer(SUCCESS, activeSendOfferTimeoutMillis, TimeUnit.MILLISECONDS);
		} catch (InterruptedException ie) {
		}
	}
	
	public void waitForCompletion() throws IOException {
		IOException	result;
		
    	LWTThreadUtil.setBlocked();
		try {
			result = queue.take();
			if (result != SUCCESS) {
				throw result;
			}
		} catch (InterruptedException ie) {
		} finally {
        	LWTThreadUtil.setNonBlocked();
		}
	}
}
