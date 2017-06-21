package com.ms.silverking.cloud.dht.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.common.SimpleValueCreator;
import com.ms.silverking.cloud.dht.daemon.PeerHealthMonitor;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.net.HostAndPort;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.net.async.MultipleConnectionQueueLengthListener;
import com.ms.silverking.net.async.PersistentAsyncServer;
import com.ms.silverking.net.async.QueueingConnectionLimitListener;
import com.ms.silverking.net.async.SelectorController;
import com.ms.silverking.time.AbsMillisTimeSource;

public class MessageGroupBase {
    private final PersistentAsyncServer<MessageGroupConnection>    paServer;
    private final byte[]        myIPAndPort;
    private final HostAndPort   myHostAndPort;
    private final IPAndPort     _myIPAndPort;
    private final AbsMillisTimeSource   deadlineTimeSource;
    private final ValueCreator  myID;
    private final MessageGroupReceiver messageGroupReceiver; // TEMP
    
    private static final boolean    debug = false;
    
    public MessageGroupBase(int port, int incomingConnectionBacklog,  
                            MessageGroupReceiver messageGroupReceiver, 
                            AbsMillisTimeSource deadlineTimeSource,
                            QueueingConnectionLimitListener limitListener, 
                            int queueLimit, 
                            int numSelectorControllers, String controllerClass,
                            MultipleConnectionQueueLengthListener mqListener, UUIDBase mqUUID) throws IOException {
        byte[] myIP;
        
        this.deadlineTimeSource = deadlineTimeSource;
        paServer = new PersistentAsyncServer<>(port, 
                             new MessageGroupConnectionCreator(messageGroupReceiver, limitListener, queueLimit), 
                             numSelectorControllers, controllerClass, mqListener, mqUUID,
                             SelectorController.defaultSelectionThreadWorkLimit);
        if (port == 0) {
        	port = paServer.getPort();
        }
        myIP = InetAddress.getLocalHost().getAddress();
        // FIXME - think about above for multi-homed hosts, may want to round-robin
        //         and/or allow user specification
        myIPAndPort = IPAddrUtil.createIPAndPort(myIP, port);
        _myIPAndPort = new IPAndPort(myIP, port);
        myHostAndPort = new HostAndPort(myIP, port);
        myID = SimpleValueCreator.forLocalProcess();
        this.messageGroupReceiver = messageGroupReceiver;
    }
    
    public MessageGroupBase(int port, 
            MessageGroupReceiver messageGroupReceiver, 
            AbsMillisTimeSource deadlineTimeSource,
            QueueingConnectionLimitListener limitListener, 
            int queueLimit, 
            int numSelectorControllers, String controllerClass) throws IOException {
    	this(port, PersistentAsyncServer.useDefaultBacklog, messageGroupReceiver, deadlineTimeSource, 
    		limitListener, queueLimit, numSelectorControllers, controllerClass, null, null);
    }
    
    public void enable() {
        paServer.enable();
    }
    
    public static IPAndPort createLocalIPAndPort(int port) {
        try {
            return new IPAndPort( IPAddrUtil.createIPAndPort(InetAddress.getLocalHost().getAddress(), port) );
        } catch (UnknownHostException uhe) {
            throw new RuntimeException("Can't read local IP address!", uhe);
        }
    }
    
    public AbsMillisTimeSource getAbsMillisTimeSource() {
        return deadlineTimeSource;
    }
    
    public byte[] getMyID() {
        return myID.getBytes();
    }
    
    public int getPort() {
        return paServer.getPort();
    }
    
    public byte[] getIPAndPort() {
        return myIPAndPort;
    }
    
    public IPAndPort _getIPAndPort() {
        return _myIPAndPort;
    }
    
    public HostAndPort getHostAndPort() {
        return myHostAndPort;
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
    
    public void send(MessageGroup mg, AddrAndPort dest) {
        if (debug) {
            System.out.println("Sending: "+ mg +" to "+ dest);
        }
        if (this._getIPAndPort().equals(dest)) { // Short circuit local
            messageGroupReceiver.receive(MessageGroup.clone(mg), null);
        } else {
            try {
                paServer.sendAsynchronous(dest.toInetSocketAddress(), mg, null, null, mg.getDeadlineAbsMillis(deadlineTimeSource));
            } catch (UnknownHostException uhe) {
                throw new RuntimeException(uhe);
            }
        }
    }
    
    public void removeAndCloseConnection(MessageGroupConnection connection) {
        paServer.removeAndCloseConnection(connection);
    }
    
    @Override
    public String toString() {
        StringBuilder   sb;
        
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
