package com.ms.silverking.cloud.dht.meta;

import com.ms.silverking.cloud.dht.daemon.storage.convergence.RingID;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.RingIDAndVersionPair;
import com.ms.silverking.cloud.toporing.InstantiatedRingTree;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfiguration;

public class DHTMetaUpdate {
    private final DHTConfiguration  dhtConfig;
    private final NamedRingConfiguration    namedRingConfig;
    private final InstantiatedRingTree	ringTree;
    private final MetaClient        metaClient;
    
    public DHTMetaUpdate(DHTConfiguration dhtConfig, NamedRingConfiguration namedRingConfig, 
                        InstantiatedRingTree ringTree, MetaClient metaClient) {
        this.dhtConfig = dhtConfig;
        this.namedRingConfig = namedRingConfig;
        this.ringTree = ringTree;
        this.metaClient = metaClient;
    }
    
    public String getDHTName() {
        return metaClient.getDHTName();
    }
    
    public DHTConfiguration getDHTConfig() {
        return dhtConfig;
    }
    
    public NamedRingConfiguration getNamedRingConfiguration() {
        return namedRingConfig;
    }

    public InstantiatedRingTree getRingTree() {
        return ringTree;
    }
    
    public MetaClient getMetaClient() {
        return metaClient;
    }
    
    public RingID getRingID() {
    	return RingID.nameToRingID(getNamedRingConfiguration().getRingName());
    }
    
    public RingIDAndVersionPair getRingIDAndVersionPair() {
        return new RingIDAndVersionPair(getRingID(), ringTree.getRingVersionPair());
    }
    
    @Override
    public String toString() {
        return getDHTName() +"\n"+ dhtConfig +"\n"+ namedRingConfig +"\n"+ ringTree;
    }
}
