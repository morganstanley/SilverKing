package com.ms.silverking.cloud.dht.daemon;

import java.io.IOException;

import com.ms.silverking.cloud.dht.daemon.MessageModule.MessageAndConnection;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupConnection;
import com.ms.silverking.thread.lwt.BaseWorker;

class MessageGroupConnectionProxyLocal implements MessageGroupConnectionProxy {
    private final BaseWorker<MessageAndConnection>  worker;
    
    MessageGroupConnectionProxyLocal(BaseWorker<MessageAndConnection> worker) {
        this.worker = worker;
    }
    
    @Override
    public void sendAsynchronous(Object data, long deadline) throws IOException {
        MessageAndConnection    messageAndConnection;
        
        //Log.warning("MessageGroupConnectionProxyLocal sending");
        messageAndConnection = new MessageAndConnection((MessageGroup)data, this);
        worker.addWork(messageAndConnection);
    }

    @Override
    public String getConnectionID() {
        return "localConnection";
    }
    
    @Override
    public MessageGroupConnection getConnection() {
    	return null;
    }    
}
