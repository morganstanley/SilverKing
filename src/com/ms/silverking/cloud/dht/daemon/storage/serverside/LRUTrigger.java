package com.ms.silverking.cloud.dht.daemon.storage.serverside;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.DHTKeyComparator;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.cloud.dht.daemon.storage.StorageValueAndParameters;
import com.ms.silverking.cloud.dht.serverside.PutTrigger;
import com.ms.silverking.cloud.dht.serverside.RetrieveTrigger;
import com.ms.silverking.cloud.dht.serverside.SSNamespaceStore;
import com.ms.silverking.cloud.dht.serverside.SSRetrievalOptions;
import com.ms.silverking.cloud.dht.serverside.SSStorageParametersAndRequirements;
import com.ms.silverking.time.AbsNanosTimeSource;
import com.ms.silverking.time.SystemTimeSource;

public class LRUTrigger implements PutTrigger, RetrieveTrigger {
	private AbsNanosTimeSource	timeSource;
	private final ConcurrentMap<DHTKey,LRUInfo>	lruInfoMap;
	private SSNamespaceStore	nsStore;	
	
	private static LRUKeyedInfoComparator	lruKeyedInfoComparator = new LRUKeyedInfoComparator();
	
	static {
		System.out.println("LRUTrigger loaded");
	}
	
	public LRUTrigger() {
		this(null);
	}
	
	public LRUTrigger(AbsNanosTimeSource timeSource) {
		this.timeSource = timeSource;
		this.lruInfoMap = new ConcurrentHashMap<>();
	}

	@Override
	public void initialize(SSNamespaceStore nsStore) {
		this.nsStore = nsStore;
		if (timeSource == null) {
			timeSource = SystemTimeSource.instance;
		}
	}

	@Override
	public ByteBuffer retrieve(SSNamespaceStore nsStore, DHTKey key, SSRetrievalOptions options) {
		LRUInfo	lruInfo;
		
		lruInfo = lruInfoMap.get(key);
		if (lruInfo != null) {
			lruInfo.updateAccessTime(timeSource.absTimeNanos());
		} else {
			// Ignore for now
		}
		return nsStore.retrieve(key, options);
	}

	@Override
	public Iterator<DHTKey> keyIterator() {
		throw new RuntimeException("Panic");
	}

	@Override
	public long getTotalKeys() {
		throw new RuntimeException("Panic");
	}

	@Override
	public OpResult put(SSNamespaceStore nsStore, DHTKey key, ByteBuffer value, SSStorageParametersAndRequirements storageParams,
						byte[] userData, NamespaceVersionMode nsVersionMode) {
		//System.out.printf("in put() %s\n", KeyUtil.keyToString(key));
		//try {
		LRUInfo	lruInfo;
		
		lruInfo = lruInfoMap.get(key);
		if (lruInfo == null) {
			lruInfo = new LRUInfo(timeSource.absTimeNanos(), storageParams.getCompressedSize());
			// Create a copy of the key to avoid referencing the incoming message
			lruInfoMap.put(new SimpleKey(key), lruInfo);
		} else {
			lruInfo.update(timeSource.absTimeNanos(), storageParams.getCompressedSize());
		}
		return nsStore.put(key, value, storageParams, userData, nsVersionMode);
		//} finally {
		//	System.out.printf("out put() %s\n", KeyUtil.keyToString(key));
		//}
	}

	@Override
	public OpResult putUpdate(SSNamespaceStore nsStore, DHTKey key, long version, byte storageState) {
		//System.out.printf("in putUpdate() %s\n", KeyUtil.keyToString(key));
		//try {
		return nsStore.putUpdate(key, version, storageState);
		//} finally {
		//	System.out.printf("out putUpdate() %s\n", KeyUtil.keyToString(key));
		//}
	}

	@Override
	public Map<DHTKey, OpResult> mergePut(List<StorageValueAndParameters> values) {
		throw new RuntimeException("Panic");
	}

	@Override
	public boolean supportsMerge() {
		return false;
	}

	@Override
	public boolean subsumesStorage() {
		return false;
	}
	
	public List<LRUKeyedInfo> getLRUList() {
		List<LRUKeyedInfo>	lruList;

		//System.out.printf("getLRUList %d\n", lruInfoMap.size());
		lruList = new ArrayList<>(lruInfoMap.size());
		for (Map.Entry<DHTKey,LRUInfo> entry : lruInfoMap.entrySet()) {
			lruList.add(new LRUKeyedInfo(entry.getKey(), entry.getValue()));
		}
		lruList.sort(lruKeyedInfoComparator);
		return lruList;
	}
	
	private static class LRUKeyedInfoComparator implements Comparator<LRUKeyedInfo> {
		LRUKeyedInfoComparator() {
		}
		
		@Override
		public int compare(LRUKeyedInfo i0, LRUKeyedInfo i1) {
			if (i0.getAccessTime() < i1.getAccessTime()) {
				return -1;
			} else if (i0.getAccessTime() > i1.getAccessTime()) {
				return 1;
			} else {
				return DHTKeyComparator.dhtKeyComparator.compare(i0.getKey(), i1.getKey());
			}
		}
	}
}
