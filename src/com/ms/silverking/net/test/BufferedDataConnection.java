package com.ms.silverking.net.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.net.async.AsyncSendListener;
import com.ms.silverking.net.async.ConnectionListener;
import com.ms.silverking.net.async.NewIncomingBufferedData;
import com.ms.silverking.net.async.OutgoingBufferedData;
import com.ms.silverking.net.async.QueueingConnection;
import com.ms.silverking.net.async.SelectorController;


/**
 * A Connection for exchanging BufferedData
 */
public class BufferedDataConnection extends QueueingConnection<OutgoingBufferedData,NewIncomingBufferedData> {
    private final BufferedDataReceiver  receiver;
    
	public BufferedDataConnection(SocketChannel channel, SelectorController<BufferedDataConnection> selectorController,
			ConnectionListener connectionListener, BufferedDataReceiver receiver) {
		super(channel, selectorController, connectionListener, true);
		this.receiver = receiver;
	}
	
    @Override
    protected NewIncomingBufferedData createIncomingData(boolean debug) {
        return new NewIncomingBufferedData(debug);
    }
		
	@Override
	protected OutgoingBufferedData wrapForSend(Object data, UUIDBase sendUUID, 
											AsyncSendListener asyncSendListener,
											long deadline) throws IOException {
		return new OutgoingBufferedData((ByteBuffer[])data, sendUUID, asyncSendListener, deadline);
	}
	
    protected void readComplete(NewIncomingBufferedData incomingData) throws IOException {
        receiver.receive(incomingData.getBuffers(), this);
    }	
}
