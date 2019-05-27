package com.ms.silverking.net.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;

import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.text.StringUtil;

/**
 * OutgoingData used by QueueingConnection. Stores a list of ByteBuffers to
 * be written to the Connection.
 */
public class OutgoingBufferedData extends OutgoingData {
	private final ByteBuffer[]	buffers;
    private long                totalBytes;
    private long                bytesWritten;
    
    private static final boolean    displayContentsForDebug = false;
		
	public OutgoingBufferedData(ByteBuffer[] buffers, UUIDBase sendUUID, 
			AsyncSendListener asyncSendListener, long deadline, Priority priority) {
		super(sendUUID, asyncSendListener, deadline, priority);
		this.buffers = buffers;
        for (ByteBuffer buffer : buffers) {
            totalBytes += buffer.remaining();
        }
        assert totalBytes != 0;
		if (AsyncGlobals.debug && Log.levelMet(Level.FINE)) {
			displayForDebug();
		}
	}
	
	public OutgoingBufferedData(ByteBuffer[] buffers, UUIDBase sendUUID, 
			AsyncSendListener asyncSendListener, long deadline) {
		this(buffers, sendUUID, asyncSendListener, deadline, Priority.NORMAL);
	}
	
	@Override
	public long getTotalBytes() {
		return totalBytes;
	}
	
	public void displayForDebug() {
		Log.warningAsync("buffers.size()\t", buffers.length);
		for (ByteBuffer buffer : buffers) {
			Log.warningAsync("buffer.remaining()\t", buffer.remaining());
			if (displayContentsForDebug) {
			    Log.warningAsync(StringUtil.byteArrayToHexString(buffer.array()));
			}
		}
	}
	
	@Override
	public boolean writeToChannel(SocketChannel channel) throws IOException {
		if (AsyncGlobals.debug && Log.levelMet(Level.FINE)) {
			Log.fine("writeToChannel ", channel);
			displayForDebug();
		}
		bytesWritten += channel.write(buffers);
		if (AsyncGlobals.debug && Log.levelMet(Level.FINE)) {
			Log.warning("writeToChannel bytesWritten / totalbytes \t"+ bytesWritten +" / "+ totalBytes);
		}
		assert bytesWritten <= totalBytes;
		return bytesWritten == totalBytes;
	}
	
	@Override
	public String toString() {
	    StringBuilder  sb;
	    
	    sb = new StringBuilder();
	    sb.append(buffers.length);
	    for (int i = 0; i < buffers.length; i++) {
	        sb.append(' ');
            sb.append(buffers[i]);
	    }
	    return sb.toString();
	}
}
