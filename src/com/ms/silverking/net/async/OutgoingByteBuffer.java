package com.ms.silverking.net.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.text.StringUtil;

/**
 * OutgoingData wrapper for ByteBuffers.
 */
public class OutgoingByteBuffer extends OutgoingData {
	private final ByteBuffer	buffer;
	private AtomicLong   totalWritten;
	
	public OutgoingByteBuffer(ByteBuffer buffer, UUIDBase sendUUID, 
								AsyncSendListener asyncSendListener,
								long deadline) {
		super(sendUUID, asyncSendListener, deadline);
		this.buffer = buffer;
	}
	
	public static OutgoingByteBuffer wrap(byte[] buf) {
		return new OutgoingByteBuffer(ByteBuffer.wrap(buf), null, null, Long.MAX_VALUE);
	}
	
	public static OutgoingByteBuffer wrap(byte[] buf, 
										UUIDBase sendUUID, 
										AsyncSendListener asyncSendListener) {
		return new OutgoingByteBuffer(ByteBuffer.wrap(buf), sendUUID, asyncSendListener, Long.MAX_VALUE);
	}
	
	public static OutgoingByteBuffer allocate(int capacity, UUIDBase sendUUID, 
												AsyncSendListener asyncSendListener) {
		return new OutgoingByteBuffer(ByteBuffer.allocate(capacity), sendUUID, asyncSendListener, Long.MAX_VALUE);
	}
	
	public long getTotalBytes() {
		return buffer.array().length;
	}
	
	@Override
	public boolean writeToChannel(SocketChannel channel) throws IOException {
	    int    written;
	    
		written = channel.write(buffer);
		totalWritten.addAndGet(written);
		if (AsyncGlobals.debug && Log.levelMet(Level.FINE)) {
		    Log.fine("written to channel: ", written +" "+ totalWritten.get());
		}
		return buffer.remaining() == 0;
	}
	
	public String toString() {
		return "OutoingByteBuffer:"+ buffer.toString();
	}
	
    public void displayForDebug() {
        Log.fine("buffer.remaining()\t", buffer.remaining());
        Log.fine(StringUtil.byteArrayToHexString(buffer.array()));
    }	
}
