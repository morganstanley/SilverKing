package com.ms.silverking.cloud.dht.net;

import java.nio.channels.SocketChannel;

import com.ms.silverking.net.async.ConnectionCreator;
import com.ms.silverking.net.async.ConnectionListener;
import com.ms.silverking.net.async.QueueingConnectionLimitListener;
import com.ms.silverking.net.async.SelectorController;
import com.ms.silverking.thread.lwt.LWTPool;

public class MessageGroupConnectionCreator implements ConnectionCreator<MessageGroupConnection> {
    private final MessageGroupReceiver  messageGroupReceiver;
    private final QueueingConnectionLimitListener   limitListener;
    private final int   queueLimit;
    
    public MessageGroupConnectionCreator(MessageGroupReceiver messageGroupReceiver, QueueingConnectionLimitListener limitListener, int queueLimit) {
        this.messageGroupReceiver = messageGroupReceiver;
        this.limitListener = limitListener;
        this.queueLimit = queueLimit;
    }
    
    public MessageGroupConnectionCreator(MessageGroupReceiver messageGroupReceiver) {
        this(messageGroupReceiver, null, Integer.MAX_VALUE);
    }
    
    @Override
    public MessageGroupConnection createConnection(SocketChannel channel, 
                                    SelectorController<MessageGroupConnection> selectorController,
                                    ConnectionListener connectionListener, LWTPool lwtPool, boolean debug) {
        return new MessageGroupConnection(channel, selectorController, connectionListener, messageGroupReceiver,
                                          limitListener, queueLimit);
    }
}
