package com.ms.silverking.net.async;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.net.async.time.RandomBackoff;
import com.ms.silverking.net.security.ConnectionAbsorbException;
import com.ms.silverking.thread.lwt.BaseWorker;
import com.ms.silverking.thread.lwt.LWTPool;
import com.ms.silverking.thread.lwt.LWTPoolParameters;
import com.ms.silverking.thread.lwt.LWTPoolProvider;

/**
 * Maintains persistent TCP connections to other peers
 */
public class PersistentAsyncServer<T extends Connection> implements IncomingConnectionListener<T>, ConnectionListener {
  private final AsyncServer<T> asyncServer;
  private final ConcurrentMap<InetSocketAddress, T> connections;
  private final ConcurrentMap<InetSocketAddress, ReentrantLock> newConnectionLocks;
  private final boolean debug;
  private final BaseWorker<OutgoingAsyncMessage> asyncConnector;
  private final NewConnectionTimeoutController newConnectionTimeoutController;
  // Client servers will refuse incoming connections
  // All communication between a client and server is initiated by the client
  // the server may only ever reply on the channel opened by the client
  // it will not open a new connection in reply - the client rejects such incoming connections
  private final boolean isClient;

  private AddressStatusProvider addressStatusProvider;
  private SuspectAddressListener suspectAddressListener;
  private boolean isRunning;
  private static final int connectionCreationAttemptTimeoutMS = 1000;
  private static final int connectionCreationMaximumTimeoutMS = 40 * 1000;
  private static final int maxConnectBackoffNum = 16;
  public static final NewConnectionTimeoutController defaultNewConnectionTimeoutController =
      new SimpleNewConnectionTimeoutController(
      maxConnectBackoffNum, connectionCreationAttemptTimeoutMS, connectionCreationMaximumTimeoutMS);

  private static final int initialConnectBackoffValue = 100;
  private static final int maxSendBackoffNum = 7;
  private static final int initialSendBackoffValue = 250;

  private static final int defaultSelectorControllers = 1;
  private static final String defaultSelectorControllerClass = "PAServer";

  public static final int useDefaultBacklog = 0;

  // Mainly used for test or inject cutomized AsyncServer
  PersistentAsyncServer(AsyncServer<T> asyncServer, NewConnectionTimeoutController newConnectionTimeoutController,
      BaseWorker<OutgoingAsyncMessage> asyncConnector, boolean debug, MultipleConnectionQueueLengthListener mqListener,
      UUIDBase mqUUID) {
    isRunning = true;
    this.debug = debug;
    this.newConnectionTimeoutController = newConnectionTimeoutController;
    this.asyncServer = asyncServer;
    connections = new ConcurrentHashMap<InetSocketAddress, T>();
    newConnectionLocks = new ConcurrentHashMap<InetSocketAddress, ReentrantLock>();
    this.asyncConnector = asyncConnector;
    if (mqListener != null) {
      new ConnectionQueueWatcher(mqListener, mqUUID);
    }
    this.isClient = false;
    //new ConnectionDebugger();
  }

  // FUTURE - Make target size and max size configurable via property
  private static final LWTPool defaultConnectorPool = LWTPoolProvider.createPool(LWTPoolParameters.create(
      "ConnectorPool").targetSize(4).maxSize(16).workUnit(1));

  public PersistentAsyncServer(int port, int backlog, int numSelectorControllers, String controllerClass,
      ConnectionCreator<T> connectionCreator, NewConnectionTimeoutController newConnectionTimeoutController,
      LWTPool readerLWTPool, LWTPool writerLWTPool, LWTPool acceptorPool, LWTPool connectorPool,
      int selectionThreadWorkLimit, boolean enabled, boolean debug, MultipleConnectionQueueLengthListener mqListener,
      UUIDBase mqUUID, boolean isClient) throws IOException {
    isRunning = true;
    this.debug = debug;
    this.newConnectionTimeoutController = newConnectionTimeoutController;
    asyncServer = new AsyncServer<T>(port, backlog, numSelectorControllers, controllerClass, connectionCreator, this,
        readerLWTPool, writerLWTPool, acceptorPool, selectionThreadWorkLimit, enabled, debug);
    connections = new ConcurrentHashMap<InetSocketAddress, T>();
    newConnectionLocks = new ConcurrentHashMap<InetSocketAddress, ReentrantLock>();
    asyncConnector = new AsyncConnector(connectorPool);
    if (mqListener != null) {
      new ConnectionQueueWatcher(mqListener, mqUUID);
    }
    this.isClient = isClient;
    //new ConnectionDebugger();
  }

  // test hook
  PersistentAsyncServer(int port, int backlog, int numSelectorControllers, String controllerClass,
      ConnectionCreator<T> connectionCreator, NewConnectionTimeoutController newConnectionTimeoutController)
      throws IOException {
    this(port, backlog, numSelectorControllers, controllerClass, connectionCreator, newConnectionTimeoutController,
        LWTPoolProvider.defaultConcurrentWorkPool, LWTPoolProvider.defaultConcurrentWorkPool,
        LWTPoolProvider.defaultConcurrentWorkPool, defaultConnectorPool,
        SelectorController.defaultSelectionThreadWorkLimit, true, false, null, null, false);
  }

  // Test hook
  public PersistentAsyncServer(int port, ConnectionCreator<T> connectionCreator) throws IOException {
    this(port, useDefaultBacklog, defaultSelectorControllers, defaultSelectorControllerClass, connectionCreator,
        defaultNewConnectionTimeoutController);
  }

  // Test hook
  public PersistentAsyncServer(int port, ConnectionCreator<T> connectionCreator, int numSelectorControllers,
      String controllerClass) throws IOException {
    this(port, useDefaultBacklog, numSelectorControllers, controllerClass, connectionCreator,
        defaultNewConnectionTimeoutController);
  }

  public PersistentAsyncServer(int port, ConnectionCreator<T> connectionCreator,
      NewConnectionTimeoutController newConnectionTimeoutController, int numSelectorControllers, String controllerClass,
      MultipleConnectionQueueLengthListener mqListener, UUIDBase mqUUID, int selectionThreadWorkLimit, boolean isClient)
      throws IOException {

    this(port, useDefaultBacklog, numSelectorControllers, controllerClass, connectionCreator,
        newConnectionTimeoutController, LWTPoolProvider.defaultConcurrentWorkPool,
        LWTPoolProvider.defaultConcurrentWorkPool, LWTPoolProvider.defaultConcurrentWorkPool, defaultConnectorPool,
        selectionThreadWorkLimit, true, false, mqListener, mqUUID, isClient);
  }

  //////////////////////////////////////////////////////////////////////

  public void enable() {
    asyncServer.enable();
  }

  public void shutdown() {
    for (Connection connection : connections.values()) {
      connection.close();
    }
    asyncServer.shutdown();
    isRunning = false;
  }

  //////////////////////////////////////////////////////////////////////

  public int getPort() {
    return asyncServer.getPort();
  }

  public void setSuspectAddressListener(SuspectAddressListener suspectAddressListener) {
    this.suspectAddressListener = suspectAddressListener;
    asyncServer.setSuspectAddressListener(suspectAddressListener);
  }

  public void setAddressStatusProvider(AddressStatusProvider addressStatusProvider) {
    if (this.addressStatusProvider != null) {
      throw new RuntimeException("Unexpected mutation of addressStatusProvider");
    }
    this.addressStatusProvider = addressStatusProvider;
        /*
        // FUTURE - make this cleaner or consider removing
        suspectAddressListener = (SuspectAddressListener)addressStatusProvider;
        asyncServer.setSuspectAddressListener(suspectAddressListener);
        */
  }

  //////////////////////////////////////////////////////////////////////

  public void sendAsynchronous(AddrAndPort dest, Object data, UUIDBase uuid, AsyncSendListener listener,
      long deadline) throws UnknownHostException {
    sendAsynchronous(dest.toInetSocketAddress(), data, uuid, listener, deadline);
  }

  public void sendAsynchronous(InetSocketAddress dest, Object data, UUIDBase uuid, AsyncSendListener listener,
      long deadline) {
    try {
      Connection connection;

      connection = getEstablishedConnection(dest);
      //if (DebugUtil.delayedDebug()) {
      //    System.out.println("getEstablishedConnection: "+ connection);
      //}
      if (connection != null) {
        connection.sendAsynchronous(data, uuid, listener, deadline);
      } else {
        newConnectionSendAsynchronous(dest, data, uuid, listener, deadline);
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
      Log.logErrorWarning(ioe);
      Log.warning("send failed: ", uuid + " " + dest);
      if (listener != null && uuid != null) {
        listener.failed(uuid);
      }
      informSuspectAddressListener(dest);
    }
  }

  private void informSuspectAddressListener(InetSocketAddress addr) {
    asyncServer.informSuspectAddressListener(addr);
  }

  public void sendSynchronous(AddrAndPort dest, Object data, UUIDBase uuid, AsyncSendListener listener,
      long deadline) throws IOException {
    sendSynchronous(dest.toInetSocketAddress(), data, uuid, listener, deadline);
  }

  public void sendSynchronous(InetSocketAddress dest, Object data, UUIDBase uuid, AsyncSendListener listener,
      long deadline) throws IOException {
    Connection connection;
    RandomBackoff backoff;

    if (uuid == null) {
      uuid = new UUIDBase();
      Log.fine("null send uuid, picking new uuid ", uuid);
      listener = null;
    }
    backoff = null;
    while (true) {
      connection = getConnectionFast(dest, deadline, null);
      try {
        connection.sendSynchronous(data, uuid, listener, deadline);
        return;
      } catch (IOException ioe) {
        connections.remove(dest);
        informSuspectAddressListener(dest);
        Log.warning(ioe + " " + dest);
        Log.logErrorWarning(ioe);
        if (backoff == null) {
          backoff = new RandomBackoff(maxSendBackoffNum, initialSendBackoffValue, deadline);
        }
        if (!backoff.maxBackoffExceeded()) {
          backoff.backoff();
        } else {
          listener.failed(uuid);
          throw ioe;
        }
      }
    }
  }

  public void send(AddrAndPort dest, Object data, boolean synchronous, UUIDBase uuid, AsyncSendListener listener,
      long deadline) throws IOException {
    send(dest.toInetSocketAddress(), data, synchronous, uuid, listener, deadline);
  }

  public void send(InetSocketAddress dest, Object data, boolean synchronous, UUIDBase uuid, AsyncSendListener listener,
      long deadline) throws IOException {
    if (synchronous) {
      sendSynchronous(dest, data, uuid, listener, deadline);
    } else {
      sendAsynchronous(dest, data, uuid, listener, deadline);
    }
  }

  public void send(AddrAndPort dest, Object data, boolean synchronous, long deadline) throws IOException {
    send(dest.toInetSocketAddress(), data, synchronous, deadline);
  }

  public void send(InetSocketAddress dest, Object data, boolean synchronous, long deadline) throws IOException {
    UUIDBase uuid;

    uuid = new UUIDBase();
    if (synchronous) {
      sendSynchronous(dest, data, uuid, null, deadline);
    } else {
      sendAsynchronous(dest, data, null, null, deadline);
    }
  }

  //////////////////////////////////////////////////////////////////////

  private Connection getEstablishedConnection(InetSocketAddress dest) throws ConnectException {
    return connections.get(dest);
  }

  public void ensureConnected(AddrAndPort dest) throws ConnectException {
    getConnection(dest,
        SystemTimeUtil.skSystemTimeSource.absTimeMillis() + newConnectionTimeoutController.getMaxRelativeTimeoutMillis(
            dest));
  }

  public Connection getConnection(AddrAndPort dest, long deadline) throws ConnectException {
    try {
      return getConnectionFast(dest.toInetSocketAddress(), deadline, null);
    } catch (UnknownHostException uhe) {
      throw new RuntimeException(uhe);
    }
  }

  private Connection getConnectionFast(InetSocketAddress dest, long deadline, String context) throws ConnectException {
    Connection connection;

    connection = connections.get(dest);
    if (connection == null) {
      connection = getConnectionSlow(dest, deadline, context);
    }
    return connection;
  }

  private Connection getConnectionSlow(InetSocketAddress dest, long deadline, String context) throws ConnectException {
    Connection connection;
    ReentrantLock destNewConnectionLock;

    destNewConnectionLock = newConnectionLocks.get(dest);
    if (destNewConnectionLock == null) {
      newConnectionLocks.putIfAbsent(dest, new ReentrantLock());
      destNewConnectionLock = newConnectionLocks.get(dest);
    }
    destNewConnectionLock.lock();
    try {
      connection = connections.get(dest);
      if (connection == null) {
        connection = createConnection(dest, deadline, context);
      }
      return connection;
    } finally {
      destNewConnectionLock.unlock();
    }
  }

  /**
   * only called when the given connection does not exist
   *
   * @return
   */
  private Connection createConnection(InetSocketAddress dest, long deadline, String context) throws ConnectException {
    RandomBackoff backoff;
    IPAndPort _dest;

    if (Log.levelMet(Level.INFO)) {
      String tag = ((isClient) ? "[Client]" : "") + "createConnection: ";
      Log.info(tag, dest);
    }

    _dest = new IPAndPort(dest);

    deadline = Math.min(deadline,
        SystemTimeUtil.skSystemTimeSource.absTimeMillis() + newConnectionTimeoutController.getMaxRelativeTimeoutMillis(
            _dest));

    backoff = null;
    while (isRunning) {
      if (addressStatusProvider != null && !addressStatusProvider.isAddressStatusProviderThread(context)
          && !addressStatusProvider.isHealthy(dest)) {
        throw new UnhealthyConnectionAttemptException("Connection attempted to unhealthy address: " + dest);
      }

      try {
        T connection;

        connection = asyncServer.newOutgoingConnection(dest, this);
        connections.putIfAbsent(dest, connection);
        if (suspectAddressListener != null) {
          suspectAddressListener.removeSuspect(dest);
        }
        return connection;
      } catch (ConnectionAbsorbException cae) {
        Log.logErrorWarning(cae, cae.getAbsorbedInfoMessage());
        if (backoff == null) {
          backoff = new RandomBackoff(newConnectionTimeoutController.getMaxAttempts(_dest), initialConnectBackoffValue,
              deadline);
        }
        if (SystemTimeUtil.skSystemTimeSource.absTimeMillis() < deadline && !backoff.maxBackoffExceeded()) {
          backoff.backoff();
        } else {
          if (suspectAddressListener != null) {
            suspectAddressListener.addSuspect(dest, SuspectProblem.ConnectionEstablishmentFailed);
          }
          throw new ConnectException(
              "Authentication fails after run out of retries [currTime=" + SystemTimeUtil.skSystemTimeSource.absTimeMillis() + ",deadline=" + deadline + "] and " + backoff + " =>" + cae.getAbsorbedInfoMessage());
        }
      } catch (ConnectException ce) {
        if (addressStatusProvider != null && addressStatusProvider.isAddressStatusProviderThread()) {
          throw new ConnectException("addressStatusProvider failed to connect: " + dest);
        }
        if (suspectAddressListener != null) {
          suspectAddressListener.addSuspect(dest, SuspectProblem.ConnectionEstablishmentFailed);
        }
        System.out.println(SystemTimeUtil.skSystemTimeSource.absTimeMillis() + "\t" + deadline);
        System.err.println(SystemTimeUtil.skSystemTimeSource.absTimeMillis() + "\t" + deadline);
        Log.warning(ce + " " + dest);
        Log.logErrorWarning(ce);
        if (backoff == null) {
          backoff = new RandomBackoff(newConnectionTimeoutController.getMaxAttempts(_dest), initialConnectBackoffValue,
              deadline);
        }
        if (SystemTimeUtil.skSystemTimeSource.absTimeMillis() < deadline && !backoff.maxBackoffExceeded()) {
          backoff.backoff();
        } else {
          informSuspectAddressListener(dest);
          throw ce;
        }
      } catch (SocketTimeoutException ste) {
        if (addressStatusProvider != null && addressStatusProvider.isAddressStatusProviderThread()) {
          throw new ConnectException("addressStatusProvider failed to connect: " + dest);
        }
        System.out.println(System.currentTimeMillis() + "\t" + deadline);
        System.err.println(System.currentTimeMillis() + "\t" + deadline);
        Log.warning(ste + " " + dest);
        Log.logErrorWarning(ste);
        if (backoff == null) {
          backoff = new RandomBackoff(newConnectionTimeoutController.getMaxAttempts(_dest), initialConnectBackoffValue,
              deadline);
        }
        if (SystemTimeUtil.skSystemTimeSource.absTimeMillis() < deadline && !backoff.maxBackoffExceeded()) {
          backoff.backoff();
        } else {
          if (suspectAddressListener != null) {
            suspectAddressListener.addSuspect(dest, SuspectProblem.ConnectionEstablishmentFailed);
          }
          throw new ConnectException(ste.toString());
        }
      } catch (IOException ioe) {
        Log.warning(ioe + " " + dest);
        Log.logErrorWarning(ioe);
        informSuspectAddressListener(dest);
        throw new RuntimeException("Unexpected IOException", ioe);
      }
    }
    return null;
  }

  @Override
  public void incomingConnection(T connection) {
    // Clients refuse incoming connections - all communication must occur on channels opened by the client
    // the server may not open a separate connection
    if (this.isClient) {
      throw new RuntimeException("Client refused an unexpected incoming connection");
    }
    connections.putIfAbsent(connection.getRemoteSocketAddress(), connection);
    connection.setConnectionListener(this);
    if (suspectAddressListener != null) {
      suspectAddressListener.removeSuspect(connection.getRemoteSocketAddress());
    }
  }

  //////////////////////////////////////////////////////////////////////

  @Override
  public void disconnected(Connection connection, InetSocketAddress remoteAddr, Object disconnectionData) {
    Log.warning("disconnected " + connection + "\t" + remoteAddr);
    removeAndCloseConnection(connection);
  }

  public void removeAndCloseConnection(Connection connection) {
    Log.warning("removeAndCloseConnection " + connection);
    if (connection.getRemoteSocketAddress() != null) {
      connections.remove(connection.getRemoteSocketAddress());
    }
  }
    
    /*
    @Override
    public void sendFailed(Connection connection, Object data) {
        Log.fine("sendFailed", connection);
        System.out.println("Send failed: "+ connection +":"+ data);
        connections.remove(connection.getChannel().socket().getRemoteSocketAddress());
    }

    @Override
    public void sendSucceeded(Connection connection, Object data) {
    }
    */

  //////////////////////////////////////////////////////////////////////

  private void newConnectionSendAsynchronous(InetSocketAddress dest, Object data, UUIDBase uuid,
      AsyncSendListener listener, long deadline) {
    asyncConnector.addWork(new OutgoingAsyncMessage(dest, data, uuid, listener, deadline));
  }

  class OutgoingAsyncMessage {
    private final InetSocketAddress dest;
    private final Object data;
    private final UUIDBase uuid;
    private final AsyncSendListener listener;
    private final long deadline;
    private final String  context;

    public OutgoingAsyncMessage(InetSocketAddress dest, Object data, UUIDBase uuid, AsyncSendListener listener,
        long deadline) {
      this.dest = dest;
      this.data = data;
      this.uuid = uuid;
      this.listener = listener;
      this.deadline = deadline;
      context = Thread.currentThread().getName();
    }

    public void failed() {
      if (listener != null) {
        Log.info("Informing of failure: ", this);
        listener.failed(uuid);
      } else {
        Log.info("No listener: ", this);
      }
    }

    public InetSocketAddress getDest() {
      return dest;
    }

    public Object getData() {
      return data;
    }

    public UUIDBase getUUID() {
      return uuid;
    }

    public AsyncSendListener getListener() {
      return listener;
    }

    public long getDeadline() {
      return deadline;
    }

    public String getContext() { return context; }

    public String toString() {
      return dest + ":" + data + ":" + uuid + ":" + listener + ":" + deadline;
    }
  }

  class AsyncConnector extends BaseWorker<OutgoingAsyncMessage> {
    public AsyncConnector(LWTPool lwtPool) {
      super(lwtPool, true, 0); // disallow direct calls to force connections to not occur in a receiving thread
    }

    @Override
    public void doWork(OutgoingAsyncMessage msg) {
      if (AsyncGlobals.debug && debug) {
        Log.fine("AsyncConnector.doSend ", msg.getUUID());
      }
      try {
        Connection connection;

        connection = getConnectionFast(msg.getDest(), msg.getDeadline(), msg.getContext());
        connection.sendAsynchronous(msg.getData(), msg.getUUID(), msg.getListener(), msg.getDeadline());
      } catch (UnhealthyConnectionAttemptException ucae) {
        Log.warning("Attempted connect to unhealthy address: ", msg.getDest());
        if (msg.getListener() != null && msg.getUUID() != null) {
          msg.getListener().failed(msg.getUUID());
        }
      } catch (IOException ioe) {
        msg.failed();
        Log.warning(ioe + " " + msg.getDest());
        Log.logErrorWarning(ioe);
        if (msg.getListener() != null && msg.getUUID() != null) {
          msg.getListener().failed(msg.getUUID());
        }
      }
    }
  }

  class ConnectionDebugger implements Runnable {
    private static final int debugIntervalMS = 10 * 1000;

    ConnectionDebugger() {
      new Thread(this).start();
    }

    private void debugConnections() {
      System.out.println();
      System.out.println("\nConnections:");
      for (T connection : connections.values()) {
        debugConnection(connection);
      }
      System.out.println();
      LWTPoolProvider.defaultConcurrentWorkPool.debug();
      System.out.println();
    }

    private void debugConnection(T connection) {
      System.out.println(connection.debugString());
    }

    @Override
    public void run() {
      while (true) {
        try {
          Thread.sleep(debugIntervalMS);
          debugConnections();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  public void writeStats() {
    asyncServer.writeStats();
  }

  class ConnectionQueueWatcher implements Runnable {
    private final UUIDBase uuid;
    private final MultipleConnectionQueueLengthListener listener;

    private static final int watchIntervalMS = 4 * 1000;

    ConnectionQueueWatcher(MultipleConnectionQueueLengthListener listener, UUIDBase uuid) {
      this.listener = listener;
      this.uuid = uuid;
      new Thread(this, "ConnectionQueueWatcher").start();
    }

    private void checkConnections() {
      int totalQueueLength;
      int longestQueueLength;
      Connection maxQueuedConnection;

      maxQueuedConnection = null;
      longestQueueLength = 0;
      totalQueueLength = 0;
      for (Connection connection : connections.values()) {
        long queueLength;

        queueLength = connection.getQueueLength();
        totalQueueLength += queueLength;
        if (queueLength > longestQueueLength) {
          maxQueuedConnection = connection;
        }
      }
      listener.queueLength(uuid, totalQueueLength, maxQueuedConnection);
    }

    @Override
    public void run() {
      while (true) {
        try {
          Thread.sleep(watchIntervalMS);
          checkConnections();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }
}
