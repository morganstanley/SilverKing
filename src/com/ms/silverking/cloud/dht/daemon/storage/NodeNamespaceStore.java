package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.concurrent.ConcurrentMap;

import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.ActiveProxyRetrieval;
import com.ms.silverking.cloud.dht.daemon.NodeRingMaster2;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.util.memory.JVMMemoryObserver;

/**
 * Provides information regarding the local DHT Node
 */
class NodeNamespaceStore extends DynamicNamespaceStore implements JVMMemoryObserver {
    private final DHTKey                nodeIDKey;
    private final DHTKey                bytesFreeKey;
    private volatile long               bytesFree;
    private final DHTKey                nsTotalKeysKey;
    private final DHTKey                nsBytesUncompressedKey;
    private final DHTKey                nsBytesCompressedKey;
    private final DHTKey                nsTotalPutsKey;
    private final DHTKey                nsTotalRetrievalsKey;
    private final Iterable<NamespaceStore>  nsStoreIterator;
    
    private static final String nsName = Namespace.nodeName;
    static final long   context = getNamespace(nsName).contextAsLong();
    
    NodeNamespaceStore(MessageGroupBase mgBase, 
            NodeRingMaster2 ringMaster, 
            ConcurrentMap<UUIDBase, ActiveProxyRetrieval> activeRetrievals,
            Iterable<NamespaceStore> nsStoreIterator) {
        super(nsName, mgBase, ringMaster, activeRetrievals);
        // static
        nodeIDKey = keyCreator.createKey("nodeID");
        // dynamic
        bytesFreeKey = keyCreator.createKey("bytesFree");
        nsTotalKeysKey = keyCreator.createKey("nsTotalKeys");
        nsBytesUncompressedKey = keyCreator.createKey("nsBytesUncompressed");
        nsBytesCompressedKey = keyCreator.createKey("nsBytesCompressed");
        nsTotalPutsKey = keyCreator.createKey("nsTotalPuts");
        nsTotalRetrievalsKey = keyCreator.createKey("nsTotalRetrievals");
        storeSystemKVPairs(mgBase, SystemTimeUtil.skSystemTimeSource.absTimeNanos());
        this.nsStoreIterator = nsStoreIterator;
    }

    private void storeSystemKVPairs(MessageGroupBase mgBase, long curTimeMillis) {
        storeStaticKVPair(mgBase, curTimeMillis, nodeIDKey, IPAddrUtil.addrAndPortToString(mgBase.getIPAndPort()));
    }
    
    protected byte[] createDynamicValue(DHTKey key, InternalRetrievalOptions options) {
        byte[]  value;
        
        value = null;
        if (key.equals(bytesFreeKey)) {
            value = Long.toString(bytesFree).getBytes();
        } else if (key.equals(nsTotalKeysKey) || key.equals(nsBytesUncompressedKey) || key.equals(nsBytesCompressedKey)) {
            StringBuilder   sb;
            
            sb = new StringBuilder();
            for (NamespaceStore nsStore : nsStoreIterator) {
                long    statValue;
                
                if (key.equals(nsTotalKeysKey)) {
                    statValue = nsStore.getTotalKeys();
                } else if (key.equals(nsBytesUncompressedKey)) {
                    statValue = nsStore.getNamespaceStats().getBytesUncompressed();
                } else if (key.equals(nsBytesCompressedKey)) {
                    statValue = nsStore.getNamespaceStats().getBytesCompressed();
                } else if (key.equals(nsTotalPutsKey)) {
                    statValue = nsStore.getNamespaceStats().getTotalPuts();
                } else if (key.equals(nsTotalRetrievalsKey)) {
                    statValue = nsStore.getNamespaceStats().getTotalRetrievals();
                } else {
                    throw new RuntimeException("panic");
                }
                sb.append(String.format("%x\t%d\n", nsStore.getNamespace(), statValue));
            }
            value = sb.toString().getBytes();
        }
        return value;
    }
    
    // JVMMemoryObserver implementation

    @Override
    public void jvmMemoryLow(boolean isLow) {
    }

    @Override
    public void jvmMemoryStatus(long bytesFree) {
        this.bytesFree = bytesFree;
    }
}
