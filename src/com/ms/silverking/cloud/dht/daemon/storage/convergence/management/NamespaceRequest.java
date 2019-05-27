package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.net.ProtoNamespaceRequestMessageGroup;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.time.SimpleTimer;
import com.ms.silverking.time.Timer;

/**
 * Tracks outstanding requests for namespaces from peers. 
 */
class NamespaceRequest {
    private final MessageGroupBase  mgBase;
    private final Set<IPAndPort>    incompletePeers;
    private final Map<UUIDBase,NamespaceRequest>  nsRequests;
    private final Set<Long>			namespaces;
    
    private static final boolean    debug = false;
    
    NamespaceRequest(MessageGroupBase mgBase, Set<IPAndPort> peers, Map<UUIDBase,NamespaceRequest> nsRequests) {
        this.mgBase = mgBase;
        this.incompletePeers = new HashSet<>(peers);
        this.nsRequests = nsRequests;
        this.namespaces = new ConcurrentSkipListSet<>();
    }
    
    public boolean waitForCompletion(int waitLimitMillis) {
        synchronized (incompletePeers) {
            Timer	timer;
            
            timer = new SimpleTimer(TimeUnit.MILLISECONDS, waitLimitMillis);
            while (incompletePeers.size() > 0 && !timer.hasExpired()) {
                try {
                    incompletePeers.wait(timer.getRemainingMillis());
                } catch (InterruptedException ie) {
                }
            }
            if (incompletePeers.size() > 0) {
                Log.warning("Unable to receive namespaces from: "+ CollectionUtil.toString(incompletePeers));
            }
            return incompletePeers.size() == 0;
        }
    }
    
    public Set<Long> getNamespaces() {
    	return namespaces;
    }
    
    public void peerComplete(IPAndPort peer, Set<Long> peerNamespaces) {
        if (debug) {
            Log.warningf("peerComplete: %s", peer);
        }
        removePeer(peer);
        namespaces.addAll(peerNamespaces);
    }
    
    private void removePeer(IPAndPort peer) {
        synchronized (incompletePeers) {
            incompletePeers.remove(peer);
            if (incompletePeers.size() == 0) {
                incompletePeers.notifyAll();
            }
        }
    }
    
    public void requestNamespacesFromPeers() {
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