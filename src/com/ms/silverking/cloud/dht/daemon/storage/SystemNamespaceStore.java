package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.daemon.ActiveProxyRetrieval;
import com.ms.silverking.cloud.dht.daemon.NodeInfo;
import com.ms.silverking.cloud.dht.daemon.NodeRingMaster2;
import com.ms.silverking.cloud.dht.meta.NodeInfoZK;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.time.SystemTimeSource;

/**
 * Provides information regarding the local DHT Node
 */
class SystemNamespaceStore extends DynamicNamespaceStore {
	private final NodeInfoZK			nodeInfoZK;
	private final NodeRingMaster2		ringMaster;
    private final DHTKey                totalDiskBytesKey;
    private final DHTKey                usedDiskBytesKey;
    private final DHTKey                freeDiskBytesKey;
    private final DHTKey                diskBytesKey;
    private final DHTKey                allReplicasFreeDiskBytesKey;
    private final DHTKey                allReplicasFreeSystemDiskBytesEstimateKey;
    private final Set<DHTKey>			knownKeys;
    
    private Map<IPAndPort,NodeInfo>	cachedAllNodeInfo;
    private long	allNodeInfoCacheTimeMS;
    private static final long	nodeInfoCacheTimeExpirationMS = 4 * 60 * 1000;
    
    private static final String nsName = Namespace.systemName;
    static final long   context = getNamespace(nsName).contextAsLong();
    
    SystemNamespaceStore(MessageGroupBase mgBase, 
            NodeRingMaster2 ringMaster, 
            ConcurrentMap<UUIDBase, ActiveProxyRetrieval> activeRetrievals,
            Iterable<NamespaceStore> nsStoreIterator, NodeInfoZK nodeInfoZK) {
        super(nsName, mgBase, ringMaster, activeRetrievals);
        this.nodeInfoZK = nodeInfoZK;
        this.ringMaster = ringMaster;
        // static
          // no static keys in this namespace at present
        // dynamic
        totalDiskBytesKey = keyCreator.createKey("totalDiskBytes");
        usedDiskBytesKey = keyCreator.createKey("usedDiskBytes");
        freeDiskBytesKey = keyCreator.createKey("freeDiskBytes");
        diskBytesKey = keyCreator.createKey("diskBytes");
        allReplicasFreeDiskBytesKey = keyCreator.createKey("allReplicasFreeDiskBytes");
        allReplicasFreeSystemDiskBytesEstimateKey = keyCreator.createKey("allReplicasFreeSystemDiskBytesEstimate");
        knownKeys = new HashSet<>();
        knownKeys.add(totalDiskBytesKey);
        knownKeys.add(usedDiskBytesKey);
        knownKeys.add(freeDiskBytesKey);
        knownKeys.add(diskBytesKey);
        knownKeys.add(allReplicasFreeDiskBytesKey);
        knownKeys.add(allReplicasFreeSystemDiskBytesEstimateKey);
    }
        
    /*
     * For all regions, determine the space available for that particular region.
     * Scale that to the entire ring. This gives an estimate of available space based 
     * on this region.
     * 
     * Take the min of all of these estimates.
     */
    
    private Map<IPAndPort,NodeInfo> getAllNodeInfo() throws KeeperException {
    	synchronized (this) {
    	    Map<IPAndPort,NodeInfo>	_cachedAllNodeInfo;
    		boolean	refresh;
    		
	    	if (cachedAllNodeInfo == null) {
	    		refresh = true;
	    	} else {
	    		refresh = SystemTimeSource.instance.absTimeMillis() > allNodeInfoCacheTimeMS + nodeInfoCacheTimeExpirationMS;
	    	}
	    	if (refresh) {
	    		_cachedAllNodeInfo = nodeInfoZK.getNodeInfo(ringMaster.getAllCurrentReplicaServers());
	    		cachedAllNodeInfo = _cachedAllNodeInfo;
		        allNodeInfoCacheTimeMS = SystemTimeSource.instance.absTimeMillis();
	    	} else {
	    		_cachedAllNodeInfo = cachedAllNodeInfo;
	    	}
	        return _cachedAllNodeInfo;
	    }
    }
    
    private Triple<Long,Long,Long> getDiskSpace() throws KeeperException {
    	Map<IPAndPort,NodeInfo>	nodeInfo;
    	long	minTotal;
    	long	minFree;
    	
    	nodeInfo = getAllNodeInfo();    	
    	minTotal = Long.MAX_VALUE;
    	minFree = Long.MAX_VALUE;
    	for (Map.Entry<IPAndPort,NodeInfo> entry : nodeInfo.entrySet()) {
    		Pair<Long,Long>	nodeEstimate;
    		
    		if (entry.getValue() != null) {
	    		nodeEstimate = getNodeEstimate(entry.getKey(), entry.getValue());
				minTotal = Math.min(minTotal, nodeEstimate.getV1());
				minFree = Math.min(minFree, nodeEstimate.getV2());
    		}
    	}
    	return new Triple<>(minTotal, minFree, minTotal - minFree);
    }    	
    
    private Pair<Long, Long> getNodeEstimate(IPAndPort node, NodeInfo info) {
		long	totalSystemBytesEstimate;
		long	freeSystemBytesEstimate;
		
		totalSystemBytesEstimate = (long)((double)info.getFSTotalBytes() / ringMaster.getCurrentOwnedFraction(node, OwnerQueryMode.Primary));
		freeSystemBytesEstimate = (long)((double)info.getFSFreeBytes() / ringMaster.getCurrentOwnedFraction(node, OwnerQueryMode.Primary));
		return new Pair<>(totalSystemBytesEstimate, freeSystemBytesEstimate);
	}

	private byte[] getAllReplicasFreeDiskBytes() {
    	List<Pair<IPAndPort,Long>>	results;
    	Map<IPAndPort,NodeInfo>		nodeInfo;
    	    	
    	results = new ArrayList<>();
    	try {
			nodeInfo = getAllNodeInfo();
	    	for (IPAndPort node : ringMaster.getAllCurrentReplicaServers()) {
	    		NodeInfo	info;
	    		
	    		info = nodeInfo.get(node);
	    		if (info != null) {
	    			results.add(new Pair<>(node, info.getFSFreeBytes()));
	    		}
	    	}
		} catch (KeeperException ke) {
			Log.logErrorWarning(ke);
		}
    	return pairedResultsToBytes(results);
    }
    
    private byte[] getAllReplicasFreeSystemDiskBytesEstimate() {
    	List<Pair<IPAndPort,Long>>	results;
    	Map<IPAndPort,NodeInfo>		nodeInfo;
    	    	
    	results = new ArrayList<>();
    	try {
			nodeInfo = getAllNodeInfo();
	    	for (IPAndPort node : ringMaster.getAllCurrentReplicaServers()) {
	    		NodeInfo	info;
	    		
	    		info = nodeInfo.get(node);	    		
	    		if (info != null) {
		    		long	freeSystemBytesEstimate;
		    		
	    			freeSystemBytesEstimate = (long)((double)info.getFSFreeBytes() / ringMaster.getCurrentOwnedFraction(node, OwnerQueryMode.Primary));
	    			results.add(new Pair<>(node, freeSystemBytesEstimate));
	    		}
	    	}
		} catch (KeeperException ke) {
			Log.logErrorWarning(ke);
		}
    	return pairedResultsToBytes(results);
    }
    
    public byte[] pairedResultsToBytes(List<Pair<IPAndPort,Long>> results) {
    	StringBuffer	sBuf;
    	
    	sBuf = new StringBuffer();
    	Collections.sort(results, SpaceComparator.instance);
    	for (Pair<IPAndPort,Long> result : results) {
        	sBuf.append(String.format("%s\t%d\n", result.getV1().getIPAsString(), result.getV2()));
    	}
    	return sBuf.toString().getBytes();
    }
    
    private static class SpaceComparator implements Comparator<Pair<IPAndPort,Long>> {
    	public SpaceComparator() {
    	}
    	
    	static final SpaceComparator	instance = new SpaceComparator();

		@Override
		public int compare(Pair<IPAndPort, Long> o1, Pair<IPAndPort, Long> o2) {
			int	result;
			
			result = Long.compare(o1.getV2(), o2.getV2());
			if (result == 0) {
				result = o1.getV1().compareTo(o2.getV1());
			}
			return result; // list in ascending order
		}
    	
    }
    
    protected byte[] createDynamicValue(DHTKey key, InternalRetrievalOptions options) {
    	try {
	        byte[]  value;
	        
	        value = null;
	        if (knownKeys.contains(key)) {
	        	Triple<Long,Long,Long>	diskSpace;
	        	
	        	diskSpace = getDiskSpace();
	            if (key.equals(totalDiskBytesKey)) {
	            	return Long.toString(diskSpace.getV1()).getBytes();
	            } else if (key.equals(freeDiskBytesKey)) {
	            	return Long.toString(diskSpace.getV2()).getBytes();
	            } else if (key.equals(usedDiskBytesKey)) {
	            	return Long.toString(diskSpace.getV3()).getBytes();
	            } else if (key.equals(diskBytesKey)) {
	            	return (diskSpace.getV1() +"\t"+ diskSpace.getV3() +"\t"+ diskSpace.getV2()).getBytes();
	            } else if (key.equals(allReplicasFreeDiskBytesKey)) {
	            	return getAllReplicasFreeDiskBytes();
	            } else if (key.equals(allReplicasFreeSystemDiskBytesEstimateKey)) {
	            	return getAllReplicasFreeSystemDiskBytesEstimate();
	            } else {
	            	throw new RuntimeException("panic");
	            }
	        }
	        return value;
    	} catch (KeeperException ke) {
    		Log.logErrorWarning(ke);
    		return null;
    	}
    }
}
