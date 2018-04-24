package com.ms.silverking.net.async;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import com.ms.silverking.log.Log;
import com.ms.silverking.thread.lwt.BaseWorker;
import com.ms.silverking.thread.lwt.LWTPool;

/**
 * Base class for implementing an asynchronous TCP/IP server.
 * Maintains persistent connections with peers.
 */
public class AsyncServer<T extends Connection> extends AsyncBase<T> {
	private final InetSocketAddress		localSocketAddr;
	private final ServerSocketChannel	serverChannel;
	private final IncomingConnectionListener<T>	incomingConnectionListener;
    private final boolean    debug;
	private boolean    enabled;
	
	public static boolean	verbose = AsyncGlobals.verbose;
		
	private AsyncServer(int port, int backlog,
						int numSelectorControllers,
						String controllerClass,
						Acceptor<T> acceptor,
						ConnectionCreator<T> connectionCreator, 
						IncomingConnectionListener<T> incomingConnectionListener, 
						LWTPool lwtPool, int selectionThreadWorkLimit, 
						boolean enabled, boolean debug) throws IOException {
		super(port, numSelectorControllers, controllerClass, acceptor, connectionCreator, lwtPool, selectionThreadWorkLimit, debug);
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
			Log.warning("AsyncServer.port: "+ getPort());
		}
	}
	    
    public AsyncServer(int port, int backlog,
            int numSelectorControllers, 
            String controllerClass,
            ConnectionCreator<T> connectionCreator, 
            IncomingConnectionListener<T> newConnectionListener, 
            LWTPool lwtPool, int selectionThreadWorkLimit, 
            boolean enabled, boolean debug) throws IOException {
        this(port, backlog, numSelectorControllers, 
                controllerClass, 
                new Acceptor<T>(lwtPool), connectionCreator, newConnectionListener, lwtPool, selectionThreadWorkLimit, enabled, debug);
    }
        
	//////////////////////////////////////////////////////////////////////
	
	public final int getPort() {
	    return serverChannel.socket().getLocalPort();
		//return localSocketAddr.getPort();
	}
	
	//////////////////////////////////////////////////////////////////////
	
	public void enable() {
	    enabled = true;
	}
	
	public void shutdown() {
		try {
			serverChannel.close();
		} catch (IOException ioe) {
			Log.logErrorWarning(ioe);
		}
		super.shutdown();
	}
		
	void accept(ServerSocketChannel channel) {
		assert channel != null;
		Log.fine("accept ", channel);
		try {
			SocketChannel   socketChannel;
			
			socketChannel = channel.accept();
			if (enabled) {
				if (socketChannel != null) {
					T	connection;
					
					connection = addConnection(socketChannel);
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
		} catch (IOException ioe) {
			Log.logErrorWarning(ioe);
		}
	}
	
	static class Acceptor<T extends Connection> extends BaseWorker<ServerSocketChannel> {
	    private AsyncServer<T>  asyncServer;
	    
	    public Acceptor(LWTPool lwtPool) {
	        super(lwtPool, true);
	    }
	    
	    public void setAsyncServer(AsyncServer<T> asyncServer) {
	        this.asyncServer = asyncServer;
	    }
	    
	    @Override
	    public void doWork(ServerSocketChannel channel) {
	        asyncServer.accept(channel);
	    }
	    
        @Override
	    public ServerSocketChannel[] newWorkArray(int size) {
	        return new ServerSocketChannel[size];
	    }
	}	
}
