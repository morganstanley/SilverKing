package com.ms.silverking.cloud.dht.daemon;

import java.io.IOException;

import com.ms.silverking.cloud.dht.net.MessageGroupConnection;

class MessageGroupConnectionProxyRemote implements MessageGroupConnectionProxy {
    private final MessageGroupConnection    connection;
    
    MessageGroupConnectionProxyRemote(MessageGroupConnection connection) {
        this.connection = connection;
    }
    
    @Override
    public void sendAsynchronous(Object data, long deadline) throws IOException {
        //Log.warning("MessageGroupConnectionProxyRemote sending to ", connection.getRemoteIPAndPort());
        connection.sendAsynchronous(data, deadline);
    }

    @Override
    public String getConnectionID() {
        return connection.getRemoteSocketAddress().toString();
    }
    
    public MessageGroupConnection getConnection() {
        return connection;
    }
}
