package com.ms.silverking.cloud.dht.daemon;


import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.net.IPAndPort;

/**
 * Given a key, provides the replicas that should store that key.
 */
public interface StorageReplicaProvider {
    IPAndPort[] getReplicas(DHTKey key, OwnerQueryMode oqm, RingOwnerQueryOpType ownerQueryOpType);
    boolean isLocal(IPAndPort replica);
}
