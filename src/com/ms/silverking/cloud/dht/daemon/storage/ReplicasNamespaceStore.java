package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.concurrent.ConcurrentMap;

import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.daemon.ActiveProxyRetrieval;
import com.ms.silverking.cloud.dht.daemon.NodeRingMaster2;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.net.IPAndPort;

/**
 * Returns the location of replicas for any given key. 
 */
class ReplicasNamespaceStore extends DynamicNamespaceStore {
    private final NodeRingMaster2    ringMaster;
    
    private static final String nsName = Namespace.replicasName;
    static final long   context = getNamespace(nsName).contextAsLong();
    private static final char   replicaDelimiter = ',';
    
    ReplicasNamespaceStore(MessageGroupBase mgBase, 
            NodeRingMaster2 ringMaster, 
            ConcurrentMap<UUIDBase, ActiveProxyRetrieval> activeRetrievals) {
        super(nsName, mgBase, ringMaster, activeRetrievals);
        this.ringMaster = ringMaster;
    }

    protected byte[] createDynamicValue(DHTKey key, InternalRetrievalOptions options) {
        StringBuilder   sb;
        
        sb = new StringBuilder();
        sb.append(KeyUtil.keyToString(key));
        sb.append('\t');
        sb.append(KeyUtil.keyToCoordinate(key));
        sb.append('\t');
        addReplicas(sb, "Primary", ringMaster.getReplicas(key, OwnerQueryMode.Primary));
        sb.append('\t');
        addReplicas(sb, "Secondary", ringMaster.getReplicas(key, OwnerQueryMode.Secondary));
        return sb.toString().getBytes();
    }
    
    private void addReplicas(StringBuilder sb, String name, IPAndPort[] replicas) {
        sb.append(name);
        sb.append(": ");
        for (int i = 0; i < replicas.length; i++) {
            sb.append(replicas[i]);
            if (i < replicas.length - 1) {
                sb.append(replicaDelimiter);
            }
        }
    }
}
