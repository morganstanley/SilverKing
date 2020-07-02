package com.ms.silverking.cloud.dht.daemon;

import java.io.IOException;
import java.util.Optional;

import com.ms.silverking.cloud.dht.daemon.MessageModule.MessageAndConnection;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupConnection;
import com.ms.silverking.thread.lwt.BaseWorker;
import com.ms.silverking.util.PropertiesHelper;

class MessageGroupConnectionProxyLocal implements MessageGroupConnectionProxy {
  private final BaseWorker<MessageAndConnection> worker;
  private static final Optional<String> connectionUserName = Optional.of(
      PropertiesHelper.systemHelper.getString("user.name"));

  MessageGroupConnectionProxyLocal(BaseWorker<MessageAndConnection> worker) {
    this.worker = worker;
  }

  @Override
  public void sendAsynchronous(Object data, long deadline) throws IOException {
    MessageAndConnection messageAndConnection;

    //Log.warning("MessageGroupConnectionProxyLocal sending");
    messageAndConnection = new MessageAndConnection((MessageGroup) data, this);
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

  @Override
  public Optional<String> getAuthenticatedUser() {
    return connectionUserName;
  }
}
