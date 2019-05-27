package com.ms.silverking.cloud.dht.net;

public interface MessageGroupReceiver {
    public void receive(MessageGroup messageGroup, MessageGroupConnection connection);
}
