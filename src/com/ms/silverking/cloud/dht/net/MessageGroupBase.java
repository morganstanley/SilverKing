package com.ms.silverking.cloud.dht.net;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;

import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.common.SimpleValueCreator;
import com.ms.silverking.cloud.dht.daemon.PeerHealthMonitor;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.net.async.AddressStatusProvider;
import com.ms.silverking.net.async.MultipleConnectionQueueLengthListener;
import com.ms.silverking.net.async.NewConnectionTimeoutController;
import com.ms.silverking.net.async.PersistentAsyncServer;
import com.ms.silverking.net.async.QueueingConnectionLimitListener;
import com.ms.silverking.net.async.SelectorController;
import com.ms.silverking.time.AbsMillisTimeSource;

public class MessageGroupBase {
  private final PersistentAsyncServer<MessageGroupConnection> paServer;
  private final IPAndPort myIPAndPort;
  private final AbsMillisTimeSource deadlineTimeSource;
  private final ValueCreator myID;
  private final MessageGroupReceiver messageGroupReceiver; // TEMP
  private final IPAliasMap  aliasMap;
  private final boolean isClient;

  private static final boolean debug = false;

  private MessageGroupBase(int interfacePort, IPAndPort myIPAndPort, int incomingConnectionBacklog, MessageGroupReceiver messageGroupReceiver,
      AbsMillisTimeSource deadlineTimeSource, NewConnectionTimeoutController newConnectionTimeoutController,
      QueueingConnectionLimitListener limitListener, int queueLimit, int numSelectorControllers, String controllerClass,
      MultipleConnectionQueueLengthListener mqListener, UUIDBase mqUUID, IPAliasMap aliasMap,
      boolean isClient) throws IOException {
    this.myIPAndPort = myIPAndPort;

    this.deadlineTimeSource = deadlineTimeSource;
    paServer = new PersistentAsyncServer<>(interfacePort,
        new MessageGroupConnectionCreator(messageGroupReceiver, limitListener, queueLimit),
        newConnectionTimeoutController, numSelectorControllers, controllerClass, mqListener, mqUUID,
        SelectorController.defaultSelectionThreadWorkLimit, isClient);
    myID = SimpleValueCreator.forLocalProcess();
    this.messageGroupReceiver = messageGroupReceiver;
    this.aliasMap = aliasMap;
    this.isClient = isClient;
  }

  public static MessageGroupBase newClientMessageGroupBase(int interfacePort, MessageGroupReceiver messageGroupReceiver,
      AbsMillisTimeSource deadlineTimeSource, NewConnectionTimeoutController newConnectionTimeoutController,
      QueueingConnectionLimitListener limitListener, int queueLimit, int numSelectorControllers, String controllerClass,
      IPAliasMap aliasMap) throws IOException {
    return new MessageGroupBase(interfacePort, new IPAndPort(IPAddrUtil.localIP(), interfacePort), PersistentAsyncServer.useDefaultBacklog, messageGroupReceiver, deadlineTimeSource,
        newConnectionTimeoutController, limitListener, queueLimit, numSelectorControllers, controllerClass, null, null,
        aliasMap, true);
  }

  public static MessageGroupBase newServerMessageGroupBase(int interfacePort, IPAndPort myIPAndPort, int incomingConnectionBacklog,
      MessageGroupReceiver messageGroupReceiver, AbsMillisTimeSource deadlineTimeSource,
      NewConnectionTimeoutController newConnectionTimeoutController, QueueingConnectionLimitListener limitListener,
      int queueLimit, int numSelectorControllers, String controllerClass,
      MultipleConnectionQueueLengthListener mqListener, UUIDBase mqUUID, IPAliasMap aliasMap)
      throws IOException {
    return new MessageGroupBase(interfacePort, myIPAndPort, PersistentAsyncServer.useDefaultBacklog, messageGroupReceiver, deadlineTimeSource,
        newConnectionTimeoutController, limitListener, queueLimit, numSelectorControllers, controllerClass, mqListener, mqUUID,
        aliasMap, false);
  }

  public void enable() {
    paServer.enable();
  }

  public boolean isClient() { return this.isClient; }

  public AbsMillisTimeSource getAbsMillisTimeSource() {
    return deadlineTimeSource;
  }

  public byte[] getMyID() {
    return myID.getBytes();
  }

  public int getInterfacePort() {
    return paServer.getPort();
  }

  public IPAndPort getIPAndPort() {
    return myIPAndPort;
  }

  public void setAddressStatusProvider(AddressStatusProvider addressStatusProvider) {
    paServer.setAddressStatusProvider(addressStatusProvider);
  }
    
    /*
    @Override
    public void receive(MessageGroup message, MessageGroupConnection connection) {
        Log.warning("\t*** Received: ", message);
        message.displayForDebug();
        for (MessageGroupEntry entry : message.getKeyIterator()) {
            System.out.println(entry);
        }
    }
    */

  private boolean isLocalDest(AddrAndPort dest) {
    return this.getIPAndPort().equals(dest);
  }

  public void send(MessageGroup mg, AddrAndPort dest) {
    if (debug) {
      Log.infof("Sending: %s to %s", mg, dest);
    }
    if (isLocalDest(dest)) { // Short circuit local
      messageGroupReceiver.receive(MessageGroup.clone(mg), null);
    } else {
      try {
        paServer.sendAsynchronous(aliasMap.daemonToInterface(dest), mg, null, null,
            mg.getDeadlineAbsMillis(deadlineTimeSource));
      } catch (UnknownHostException uhe) {
        throw new RuntimeException(uhe);
      }
    }
  }

  public void ensureConnected(AddrAndPort dest) throws ConnectException {
    paServer.ensureConnected(aliasMap.daemonToInterface(dest));
  }

  public MessageGroupConnection getConnection(AddrAndPort dest, long deadline) throws ConnectException {
    return (MessageGroupConnection) paServer.getConnection(aliasMap.daemonToInterface(dest), deadline);
  }

  public void removeAndCloseConnection(MessageGroupConnection connection) {
    paServer.removeAndCloseConnection(connection);
  }

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append(paServer.getPort());
    return sb.toString();
  }

  public void writeStats() {
    paServer.writeStats();
  }

  public void shutdown() {
    paServer.shutdown();
  }

  public void setPeerHealthMonitor(PeerHealthMonitor peerHealthMonitor) {
    paServer.setSuspectAddressListener(peerHealthMonitor);
  }
}
