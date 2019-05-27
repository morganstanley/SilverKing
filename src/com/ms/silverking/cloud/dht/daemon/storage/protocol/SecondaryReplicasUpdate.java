package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import java.util.List;

import com.ms.silverking.net.IPAndPort;

public class SecondaryReplicasUpdate {
    private final List<IPAndPort>   replicas;
    private final RetrievalResult   result;
    
    public SecondaryReplicasUpdate(List<IPAndPort> replicas, RetrievalResult result) {
        this.replicas = replicas;
        this.result = result;
    }
    
    public List<IPAndPort> getReplicas() {
        return replicas;
    }

    public RetrievalResult getResult() {
        return result;
    }
}
