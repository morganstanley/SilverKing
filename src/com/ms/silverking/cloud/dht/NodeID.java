package com.ms.silverking.cloud.dht;

import com.ms.silverking.net.IPAndPort;

// Consider deprecating

/**
 * Unique identifier of a DHT node (server/daemon).
 */
public class NodeID implements Comparable<NodeID> {
    private final IPAndPort   ipAndPort;
    
    public NodeID(IPAndPort ipAndPort) {
        this.ipAndPort = ipAndPort;
    }
    
    @Override
    public int hashCode() {
        return ipAndPort.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        NodeID  oID;
        
        oID = (NodeID)other;
        return this.ipAndPort.equals(oID.ipAndPort);
    }
    
    @Override
    public int compareTo(NodeID o) {
        return this.ipAndPort.compareTo(o.ipAndPort);
    }
    
    @Override 
    public String toString() {
        return ipAndPort.toString();
    }
}
