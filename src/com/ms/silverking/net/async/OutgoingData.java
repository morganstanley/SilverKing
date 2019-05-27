package com.ms.silverking.net.async;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.time.AbsMillisTimeSource;

/**
 * OutgoingData base implementation. Associates data with a unique send id, 
 * the deadline by which the data should be sent, and a callback to 
 * be informed upon successful send or upon failure.
 */
public abstract class OutgoingData {
	private final UUIDBase 			sendUUID; // FUTURE - probably change this from uuid to long
	                                          // uuid is really overkill for how this is used
	private final AsyncSendListener	sendListener;
    private final long              creationTime;
	private final long				deadline;
	private final Priority			priority;
	
	private static AbsMillisTimeSource absMillisTimeSource;
	
	public static final int minRelativeDeadline = 5;

	/**
	 * Must be called to set the local time source.
	 * @param _absMillisTimeSource
	 */
	public static void setAbsMillisTimeSource(AbsMillisTimeSource _absMillisTimeSource) {
	    absMillisTimeSource = _absMillisTimeSource;
	}
	
	public enum Priority {NORMAL, HIGH};
	
	public OutgoingData(UUIDBase sendUUID, AsyncSendListener sendListener, long deadline, Priority priority) {
		this.sendUUID = sendUUID;
		this.sendListener = sendListener;
		this.deadline = deadline;
		this.priority = priority;
		creationTime = absMillisTimeSource.absTimeMillis();
        if (deadline <= creationTime) {
        	Log.warningf("deadline <= creationTime; %d < %d", deadline, creationTime);
        	//throw new RuntimeException("deadline <= creationTime");
        	Thread.dumpStack();
        	deadline = creationTime + minRelativeDeadline;
        }		
	}
	
	public OutgoingData(UUIDBase sendUUID, AsyncSendListener sendListener, long deadline) {
		this(sendUUID, sendListener, deadline, Priority.NORMAL);
	}
	
	public final UUIDBase getSendUUID() {
		return sendUUID;
	}
	
	public final long getDeadline() {
		return deadline;
	}
	
	public final boolean deadlineExpired() {
		return absMillisTimeSource.absTimeMillis() > deadline;
	}
	
	public final Priority getPriority() {
		return priority;
	}
	
	public abstract boolean writeToChannel(SocketChannel channel) throws IOException;
	public abstract long getTotalBytes();
	
	protected final void successful() {
		if (sendListener != null && sendUUID != null) {
			sendListener.sent(sendUUID);
		}
	}
	
	protected final void failed() {
		if (sendListener != null && sendUUID != null) {
			sendListener.failed(sendUUID);
		}
	}
	
	protected final void timeout() {
		if (sendListener != null && sendUUID != null) {
			sendListener.timeout(sendUUID);
		}
	}
	
    public void displayForDebug() {
        Log.warningAsync(String.format("creationTime %d deadline %d curTimeMillis %d %s\n", 
                creationTime, deadline, absMillisTimeSource.absTimeMillis(),
                absMillisTimeSource.absTimeMillis() <= deadline ? "Data has NOT timed out" : "Data has timed out!"));
    }
 }
