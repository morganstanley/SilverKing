package com.ms.silverking.cloud.dht.net;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMap;
import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.SimpleValueCreator;
import com.ms.silverking.cloud.dht.daemon.PeerHealthMonitor;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
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
    private final Map<AddrAndPort,AddrAndPort[]>	addrAliases;
    private final AtomicInteger	aliasIndex; 
    
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
        aliasIndex = new AtomicInteger();
        addrAliases = readAliases(port);
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
    
    private Map<AddrAndPort,AddrAndPort[]> readAliases(int port) {
    	String	aliasMapFile;
    	
		aliasMapFile = DHTConstants.defaultDefaultClassVars.getVarMap().get(DHTConstants.ipAliasMapFileVar);
		Log.warningf("%s=%s", DHTConstants.ipAliasMapFileVar, aliasMapFile);
    	if (aliasMapFile != null && aliasMapFile.trim().length() > 0) {
    		return readAliasMap(aliasMapFile, port);
    	} else {
    		return null;
    	}
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
            	AddrAndPort	_dest;
            	
            	if (addrAliases != null) {
            		AddrAndPort[]	aliases;
            		
            		aliases = addrAliases.get(dest);
            		if (aliases != null) {
            			//_dest = aliases[ThreadLocalRandom.current().nextInt(aliases.length)];
            			_dest = aliases[aliasIndex.getAndIncrement() % aliases.length];
            			//System.out.printf("%s ==> %s\n", dest, _dest);
            		} else {
            			_dest = dest;
            		}
            	} else {
            		_dest = dest;
            	}
                paServer.sendAsynchronous(_dest.toInetSocketAddress(), mg, null, null, mg.getDeadlineAbsMillis(deadlineTimeSource));
            } catch (UnknownHostException uhe) {
                throw new RuntimeException(uhe);
            }
        }
    }
    
	public MessageGroupConnection getConnection(AddrAndPort dest, long deadline) throws ConnectException {
		return (MessageGroupConnection)paServer.getConnection(dest, deadline);
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
    
    private Map<AddrAndPort,AddrAndPort[]> readAliasMap(String f, int port) {
    	try {
    		return readAliasMap(new File(f), port);
    	} catch (IOException ioe) {
    		Log.logErrorWarning(ioe, "Unable to read ip alias map: "+ f);
    		return null;
    	}
    }
    
    private Map<AddrAndPort,AddrAndPort[]> readAliasMap(File f, int port) throws IOException {
    	return readAliasMap(new FileInputStream(f), port);
    }
    
    private Map<AddrAndPort,AddrAndPort[]> readAliasMap(InputStream in, int port) throws IOException {
    	BufferedReader	reader;
    	String			line;
    	HashMap<AddrAndPort,AddrAndPort[]>	map;
    	
    	map = new HashMap<>();
    	reader = new BufferedReader(new InputStreamReader(in));
    	do {
        	line = reader.readLine();
        	if (line != null) {
        		Pair<AddrAndPort,AddrAndPort[]>	entry;
        		
        		entry = readAliasMapEntry(line, port);
        		map.put(entry.getV1(), entry.getV2());
        	}
    	} while (line != null);
    	reader.close();
    	return ImmutableMap.copyOf(map);
    }
    
    private Pair<AddrAndPort,AddrAndPort[]> readAliasMapEntry(String s, int port) {
    	String[]		toks;
    	
    	toks = s.trim().split("\\s+");
    	if (toks.length != 2) {
    		throw new RuntimeException("Invalid map entry: "+ s);
    	} else {
        	AddrAndPort		addr;
        	AddrAndPort[]	aliases;
        	
        	addr = new IPAndPort(toks[0], port);
        	aliases = IPAndPort.parseToArray(toks[1], port);
        	return new Pair<>(addr, aliases);
    	}
    }
}
