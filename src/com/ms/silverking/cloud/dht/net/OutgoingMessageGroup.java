package com.ms.silverking.cloud.dht.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.net.protocol.MessageFormat;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.io.util.ChannelUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.async.AsyncGlobals;
import com.ms.silverking.net.async.AsyncSendListener;
import com.ms.silverking.net.async.OutgoingData;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.text.StringUtil;

/*
 * Extension of Outgoing data to support MessageGroups.
 */
public class OutgoingMessageGroup extends OutgoingData {
    private final MessageGroup    messageGroup;
    private final ByteBuffer[]    buffers;
    private final long            totalBytes;
    
    private long    bytesWritten;
    
    private static final int    numHeaderBuffers = 2;
    private static final int    leadingBufferIndex = 0;
    private static final int    bufferLengthsBufferIndex = 1;
    
    private static final boolean    displayContentsForDebug = false;
    
    // alternating
    public OutgoingMessageGroup(MessageGroup messageGroup, UUIDBase sendUUID, 
                AsyncSendListener asyncSendListener, long deadline, Priority priority) {
        super(sendUUID, asyncSendListener, deadline, priority);
        this.messageGroup = messageGroup;
        buffers = createBuffers(messageGroup);
        this.totalBytes = computeTotalBytes(buffers);
    }
    
    @Override
    public long getTotalBytes() {
        return totalBytes;
    }
    
    private static long computeTotalBytes(ByteBuffer[] buffers) {
        long    totalBytes;
        
        totalBytes = 0;
        for (ByteBuffer buffer : buffers) {
            totalBytes += buffer.remaining();
        }
        return totalBytes;
    }
        
    private static ByteBuffer[] createBuffers(MessageGroup messageGroup) {
        ByteBuffer[]    msg;
        ByteBuffer[]    mgBuffers;
        byte[]          bufferLengthsArray;
        ByteBuffer      leadingBuffer;
        byte[]          originator;
        
        // FUTURE - think about the use of fewer buffers
        // for instance, one simple optimization is to combine
        // the leadingbuffer and buffer lengths buffer
        
        // we add two buffers to the buffers in the message group:
        // 0) leadingBuffer which contains the message header
        // 1) a buffer lengths buffer containing the length of all
        // user byte buffers

        mgBuffers = messageGroup.getBuffers();
        assert mgBuffers.length > 0;

        leadingBuffer = ByteBuffer.allocate(MessageFormat.leadingBufferSize);
        leadingBuffer.put(MessageGroupGlobals.preamble);
        leadingBuffer.put(MessageGroupGlobals.protocolVersion);
        leadingBuffer.putInt(mgBuffers.length);
        leadingBuffer.put((byte)messageGroup.getMessageType().ordinal());
        leadingBuffer.put((byte)messageGroup.getOptions()); // only use low byte of options for now
        leadingBuffer.putShort((short)0); // ignore two bytes of options for now
        leadingBuffer.putLong(messageGroup.getUUID().getMostSignificantBits());
        leadingBuffer.putLong(messageGroup.getUUID().getLeastSignificantBits());
        leadingBuffer.putLong(messageGroup.getContext());
        originator = messageGroup.getOriginator();
        if (originator.length != ValueCreator.BYTES) {
            throw new RuntimeException("originator.length != ValueCreator.BYTES\t"+
                    originator.length +" "+ ValueCreator.BYTES);
        }
        leadingBuffer.put(originator);
        // For debugging
        //Thread.dumpStack();
        //System.out.printf("%x:%x %s messageGroup.getDeadlineRelativeMillis() %d\n",
        //        messageGroup.getUUID().getMostSignificantBits(), messageGroup.getUUID().getLeastSignificantBits(),
        //        IPAddrUtil.addrAndPortToString(messageGroup.getOriginator()), messageGroup.getDeadlineRelativeMillis());
        leadingBuffer.putInt(messageGroup.getDeadlineRelativeMillis());
        leadingBuffer.put((byte)messageGroup.getForwardingMode().ordinal());
        leadingBuffer.position(0);
        
        msg = new ByteBuffer[numHeaderBuffers + mgBuffers.length];
        msg[leadingBufferIndex] = leadingBuffer;
        bufferLengthsArray = new byte[mgBuffers.length * NumConversion.BYTES_PER_INT];
        msg[bufferLengthsBufferIndex] = ByteBuffer.wrap(bufferLengthsArray);
        for (int i = 0; i < mgBuffers.length; i++) {
            NumConversion.intToBytes(mgBuffers[i].remaining(), bufferLengthsArray, 
                                     i * NumConversion.BYTES_PER_INT);
            msg[numHeaderBuffers + i] = mgBuffers[i];
        }                
        // FUTURE - think about whether to create buffers here to pipeline more...
        return msg;
    }    
    
    //private final long          creationTime = System.currentTimeMillis();
    private final int           myID = nextID.getAndIncrement();
    private final AtomicInteger writes = new AtomicInteger();
    private static final AtomicInteger  nextID = new AtomicInteger(); 
    
    @Override
    public boolean writeToChannel(SocketChannel channel) throws IOException {
        if (AsyncGlobals.debug && Log.levelMet(Level.FINE)) {
            Log.fine("writeToChannel ", channel);
            Log.fine("writes: ", writes.getAndIncrement());
            Log.fine("myID: ", myID);
            Log.fine("this: ", this);
            //Log.fine("creationTime: ", creationTime);
            Log.fine("time: ", System.currentTimeMillis());
            Thread.dumpStack();
            displayForDebug();
        }
        //bytesWritten += channel.write(buffers);
        bytesWritten += ChannelUtil.writeBuffersBatched(buffers, channel);
        if (AsyncGlobals.debug && Log.levelMet(Level.FINE)) {
            Log.fine("writeToChannel bytesWritten / totalbytes \t"+ bytesWritten +" / "+ totalBytes);
        }
        assert bytesWritten <= totalBytes;
        //if (bytesWritten > totalBytes) {
        //    Log.warning("OutgoingMessageGroup bytesWritten > totalBytes");
        //    displayForDebug();
        //    assert bytesWritten <= totalBytes;
        //}
        return bytesWritten == totalBytes;
    }    
    
    public void displayForDebug() {
        super.displayForDebug();
        Log.fine("buffers.size()\t", messageGroup.getBuffers().length);
        for (ByteBuffer buffer : messageGroup.getBuffers()) {
            Log.warning("buffer.remaining()\t", buffer.remaining());
            if (displayContentsForDebug) {
                Log.warning(StringUtil.byteBufferToHexString(buffer));
            }
        }
    }
}
