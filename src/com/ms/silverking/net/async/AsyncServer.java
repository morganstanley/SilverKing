package com.ms.silverking.net.async;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import com.ms.silverking.collection.Triple;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.security.ConnectionAbsorbException;
import com.ms.silverking.thread.lwt.BaseWorker;
import com.ms.silverking.thread.lwt.LWTPool;

/**
 * Base class for implementing an asynchronous TCP/IP server.
 * Maintains persistent connections with peers.
 */
public class AsyncServer<T extends Connection> extends AsyncBase<T> {
  private final InetSocketAddress localSocketAddr;
  private ServerSocketChannel serverChannel;
  private final IncomingConnectionListener<T> incomingConnectionListener;
  private final boolean debug;
  private boolean enabled;

  public static boolean verbose = AsyncGlobals.verbose;

  private AsyncServer(int port, int backlog, int numSelectorControllers, String controllerClass, Acceptor<T> acceptor,
      ConnectionCreator<T> connectionCreator, IncomingConnectionListener<T> incomingConnectionListener,
      LWTPool readerLWTPool, LWTPool writerLWTPool, int selectionThreadWorkLimit, boolean enabled, boolean debug)
      throws IOException {
    super(port, numSelectorControllers, controllerClass, acceptor, connectionCreator, readerLWTPool, writerLWTPool,
        selectionThreadWorkLimit, debug);

    this.incomingConnectionListener = incomingConnectionListener;
    this.enabled = enabled;
    this.debug = debug;

    acceptor.setAsyncServer(this);

    // Create a new non-blocking server socket channel
    serverChannel = ServerSocketChannel.open();

    // Bind the server socket to the specified address and port
    localSocketAddr = new InetSocketAddress(port);
    serverChannel.socket().bind(localSocketAddr, backlog);

    // Now add the server channel to the AsyncBase
    addServerChannel(serverChannel);

    if (verbose) {
      Log.warning("AsyncServer.port: " + getPort());
    }
  }

  public AsyncServer(int port, int backlog, int numSelectorControllers, String controllerClass,
      ConnectionCreator<T> connectionCreator, IncomingConnectionListener<T> newConnectionListener,
      LWTPool readerLWTPool, LWTPool writerLWTPool, LWTPool acceptorPool, int selectionThreadWorkLimit, boolean enabled,
      boolean debug) throws IOException {
    this(port, backlog, numSelectorControllers, controllerClass, new Acceptor<T>(acceptorPool), connectionCreator,
        newConnectionListener, readerLWTPool, writerLWTPool, selectionThreadWorkLimit, enabled, debug);
  }

  //////////////////////////////////////////////////////////////////////

  public final int getPort() {
    return serverChannel.socket().getLocalPort();
  }

  //////////////////////////////////////////////////////////////////////

  public void enable() {
    enabled = true;
  }

  public void shutdown() {
    try {
      if (serverChannel != null) {
        serverChannel.close();
        serverChannel = null;
      }
    } catch (IOException ioe) {
      Log.logErrorWarning(ioe);
    }
    super.shutdown();
  }

  void accept(ServerSocketChannel channel) {
    SocketChannel socketChannel = null;
    boolean connectionSuccess = false;

    assert channel != null;
    Log.fine("accept ", channel);

    try {
      socketChannel = channel.accept();
      if (enabled) {
        if (socketChannel != null) {
          T connection;

          connection = addConnection(socketChannel, true);
          incomingConnectionListener.incomingConnection(connection);
        } else {
          if (AsyncGlobals.debug && debug) {
            Log.info("null socketChannel");
          }
        }
      } else {
        if (socketChannel != null) {
          // if we're not enabled, we are not yet
          // able to process incoming connections
          socketChannel.close();
        }
      }
      connectionSuccess = true;
    } catch (ConnectionAbsorbException cae) {
      Log.logErrorWarning(cae, cae.getAbsorbedInfoMessage());
    } catch (IOException ioe) {
      Log.logErrorWarning(ioe);
    } finally {
      if (!connectionSuccess && socketChannel != null) {
        try {
          socketChannel.close();
        } catch (IOException e) {
          Log.logErrorWarning(e, "Could not close socketChannel " + socketChannel);
        }
      }
    }
  }

  static class Acceptor<T extends Connection>
      extends BaseWorker<Triple<ServerSocketChannel, SelectorController<T>, SelectionKey>> {
    private AsyncServer<T> asyncServer;

    public Acceptor(LWTPool lwtPool) {
      super(lwtPool, true);
    }

    public void setAsyncServer(AsyncServer<T> asyncServer) {
      this.asyncServer = asyncServer;
    }

    @Override
    public void doWork(Triple<ServerSocketChannel, SelectorController<T>, SelectionKey> work) {
      ServerSocketChannel channel;
      SelectorController<T> selectorController;
      SelectionKey key;

      channel = work.getV1();
      selectorController = work.getV2();
      key = work.getV3();
      try {
        asyncServer.accept(channel);
      } finally {
        selectorController.addSelectionKeyOps(key, SelectionKey.OP_ACCEPT);
      }
    }

    @Override
    public Triple<ServerSocketChannel, SelectorController<T>, SelectionKey>[] newWorkArray(int size) {
      return new Triple[size];
    }
  }
}
