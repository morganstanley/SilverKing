package com.ms.silverking.net.async;

import java.net.InetSocketAddress;
import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.ms.silverking.net.InetAddressUtil;


/**
 * Reserve a single SelectorController for all local connections. Spread others over selector controllers
 * grouped by address.
 */
public class LocalGroupingCSCA<T extends Connection> implements ChannelSelectorControllerAssigner<T> {
    private final int   numLocalSelectors;
    private final AtomicInteger nextLocalIndex;
    private final AtomicInteger nextNonLocalIndex;
    
    private static final int    defaultNumLocalSelectors = 1;
    private static final boolean	debug = false;
    
	public LocalGroupingCSCA(int numLocalSelectors) {
	    if (numLocalSelectors < 0) {
	        throw new RuntimeException("numLocalSelectors < 0");
	    }
	    this.numLocalSelectors = numLocalSelectors;
	    nextLocalIndex = new AtomicInteger();
        nextNonLocalIndex = new AtomicInteger();
	}
	
    public LocalGroupingCSCA() {
        this(defaultNumLocalSelectors);
    }
    
	@Override
	/**
	 * Assigns, the given channel to a SelectorController chosen from the list.
	 * Does *not* add the channel to the SelectorController.
	 */
	public SelectorController<T> assignChannelToSelectorController(SelectableChannel channel,
			List<SelectorController<T>> selectorControllers) {
		boolean   isLocal;
        int       index;

        if (selectorControllers.size() <= numLocalSelectors) {
            throw new RuntimeException("selectorControllers.size() <= numLocalSelectors");
        }
        if (numLocalSelectors == 0) {
            isLocal = false;
            if (debug) {
            	System.out.printf("no local selectors\n");
            }
        } else {
    		if (channel instanceof ServerSocketChannel) {
    		    //index = localIndex;
    		    isLocal = true;
                if (debug) {
                	System.out.printf("ServerSocketChannel\n");
                }
    		} else {
    		    SocketChannel     socketChannel;
    		    InetSocketAddress socketAddress;
    		    
    		    socketChannel = (SocketChannel)channel;
    		    socketAddress = (InetSocketAddress)socketChannel.socket().getRemoteSocketAddress();
    		    if (InetAddressUtil.isLocalHostIP(socketAddress.getAddress())) {
    		        //index = localIndex;
    		        isLocal = true;
    		    } else {
                    //index = socketAddress.hashCode() % (selectorControllers.size() - numLocalSelectors) + numLocalSelectors;
    		        isLocal = false;
    		    }
                if (debug) {
                	System.out.printf("socketAddress %s\n", socketAddress);
                }
    		}
        }
		if (isLocal) {
		    index = nextLocalIndex.getAndIncrement() % numLocalSelectors;
		} else {
		    int   numNonLocalSelectorControllers;
		    
		    numNonLocalSelectorControllers = selectorControllers.size() - numLocalSelectors;
            assert numNonLocalSelectorControllers > 0;
            index = numLocalSelectors + (nextNonLocalIndex.getAndIncrement() % numNonLocalSelectorControllers);
		}
        if (debug) {
        	System.out.printf("%s index %d  sc %d  local %d  nonlocal %d %s\n", isLocal, index, selectorControllers.size(), 
		        numLocalSelectors, selectorControllers.size() - numLocalSelectors,
		        selectorControllers.get(index).getThreadName());
        }
		return selectorControllers.get(index);
	}
}
