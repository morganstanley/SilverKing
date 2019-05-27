package com.ms.silverking.collection;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentMap;

import com.ms.silverking.log.Log;

/**
 * Iterates through all entries of a nested concurrent map and removes 
 * entries that are empty.
 */
public class NestedConcurrentMapCleaner<K,L,V> extends TimerTask {
    private final ConcurrentMap<K,ConcurrentMap<L,V>>   map;
    private final boolean   verbose;
    private final Set<K>    candidates;
    
    public NestedConcurrentMapCleaner(ConcurrentMap<K,ConcurrentMap<L,V>> map,
                                      Timer timer, long periodMillis, boolean verbose) {
        this.map = map;
        timer.scheduleAtFixedRate(this, periodMillis, periodMillis);
        this.verbose = verbose;
        candidates = new HashSet<>();
    }
    
    public NestedConcurrentMapCleaner(ConcurrentMap<K,ConcurrentMap<L,V>> map,
            Timer timer, long periodMillis) {
        this(map, timer, periodMillis, false);
    }
    
    public void run() {
        int size;

        if (verbose) {
            //System.gc();
            Log.warning("Cleaning: ", map);
        }
        size = 0;
        for (Map.Entry<K,ConcurrentMap<L,V>> entry : map.entrySet()) {
            ConcurrentMap<L,V>  nestedMap;
            
            size++;
            nestedMap = entry.getValue();
            if (nestedMap.size() == 0) {
                if (verbose) {
                    Log.warning("Found empty nested map. Removing key: ", entry.getKey());
                }
                // "Candidate" code is used to work around race condition between
                // map creation and map population. This only works for maps which are quickly populated
                if (candidates.contains(entry.getKey())) {
                    candidates.remove(entry.getKey());
                    map.remove(entry.getKey());
                } else {
                    candidates.add(entry.getKey());
                }
                
            }
        }
        if (verbose) {
            Log.warning("size: ", size);
            //System.gc();
            //Log.warning("MB Free: "+ (Runtime.getRuntime().freeMemory() / (1024 * 1024)));
        }
    }
}
