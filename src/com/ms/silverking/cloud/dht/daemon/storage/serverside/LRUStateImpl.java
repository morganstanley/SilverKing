package com.ms.silverking.cloud.dht.daemon.storage.serverside;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.DHTKeyComparator;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.time.AbsNanosTimeSource;
import com.ms.silverking.time.SystemTimeSource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LRUStateImpl implements LRUStateProvider {
    private AbsNanosTimeSource    timeSource;
    private final ConcurrentMap<DHTKey, LRUInfo>    lruInfoMap;
    private static LRUKeyedInfoComparator lruKeyedInfoComparator = new LRUKeyedInfoComparator();

    public LRUStateImpl(AbsNanosTimeSource timeSource) {
        if (timeSource == null) {
            this.timeSource = SystemTimeSource.instance;
        } else {
            this.timeSource = timeSource;
        }
        this.lruInfoMap = new ConcurrentHashMap<>();
    }

    public void markRead(DHTKey key) {
        LRUInfo    lruInfo;

        lruInfo = lruInfoMap.get(key);
        if (lruInfo != null) {
            lruInfo.updateAccessTime(timeSource.absTimeNanos());
        } else {
            // Ignore for now
        }
    }

    public void markPut(DHTKey key, int compressedSize) {
        LRUInfo    lruInfo;

        lruInfo = lruInfoMap.get(key);
        if (lruInfo == null) {
            lruInfo = new LRUInfo(timeSource.absTimeNanos(), compressedSize);
            // Create a copy of the key to avoid referencing the incoming message
            lruInfoMap.put(new SimpleKey(key), lruInfo);
        } else {
            lruInfo.update(timeSource.absTimeNanos(), compressedSize);
        }
    }

    public List<LRUKeyedInfo> getLRUList() {
        List<LRUKeyedInfo>    lruList;

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
