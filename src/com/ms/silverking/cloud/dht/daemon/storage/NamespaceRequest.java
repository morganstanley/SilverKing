package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.net.ProtoNamespaceRequestMessageGroup;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

/**
 * Tracks outstanding requests for namespaces from peers. 
 */
class NamespaceRequest {
    private MessageGroupBase    mgBase;
    private Set<IPAndPort>      incompletePeers;
    private Map<UUIDBase,NamespaceRequest>  nsRequests;
    
    private static final int    waitLimitMillis = 2 * 60 * 1000;    
    
    private static final boolean    debug = false;
    
    NamespaceRequest(MessageGroupBase mgBase, Set<IPAndPort> peers, Map<UUIDBase,NamespaceRequest> nsRequests) {
        this.mgBase = mgBase;
        this.incompletePeers = new HashSet<>(peers);
        this.nsRequests = nsRequests;
    }
    
    void waitForCompletion() {
        synchronized (incompletePeers) {
            Stopwatch   sw;
            
            sw = new SimpleStopwatch();
            while (incompletePeers.size() > 0 && sw.getSplitMillis() < waitLimitMillis) {
                try {
                    incompletePeers.wait(waitLimitMillis);
                } catch (InterruptedException ie) {
                }
            }
            if (incompletePeers.size() > 0) {
                Log.warning("Unable to receive namespaces from: "+ CollectionUtil.toString(incompletePeers));
            }
        }
    }
    
    void peerComplete(IPAndPort peer) {
        removePeer(peer);
    }
    
    private void removePeer(IPAndPort peer) {
        synchronized (incompletePeers) {
            incompletePeers.remove(peer);
            if (incompletePeers.size() == 0) {
                incompletePeers.notifyAll();
            }
        }
    }
    
    void requestNamespacesFromPeers() {
        Set<IPAndPort>  _incompletePeers;
        
        synchronized (incompletePeers) {
            _incompletePeers = ImmutableSet.copyOf(incompletePeers);
        }
        for (IPAndPort peer : _incompletePeers) {
            sendNamespaceRequest(peer);
        }
    }
    
    private void sendNamespaceRequest(IPAndPort dest) {
        ProtoNamespaceRequestMessageGroup   protoMG;
        
        if (debug) {
            Log.warning("Requesting namespaces from: ", dest);
        }
        protoMG = new ProtoNamespaceRequestMessageGroup(new UUIDBase(), mgBase.getMyID());
        nsRequests.put(protoMG.getUUID(), this);
        mgBase.send(protoMG.toMessageGroup(), dest);
    }
}