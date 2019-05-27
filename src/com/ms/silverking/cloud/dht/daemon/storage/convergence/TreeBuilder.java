package com.ms.silverking.cloud.dht.daemon.storage.convergence;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.daemon.storage.KeyAndVersionChecksum;
import com.ms.silverking.cloud.ring.RingRegion;

/**
 * Builds checksum trees for multiple regions. This allows all desired trees to be built
 * in a single pass over local data. 
 */
public class TreeBuilder {
    private final Iterator<KeyAndVersionChecksum>       keyIterator;
    private final NavigableMap<Long,RegionTreeBuilder>  rtBuilders;
    
    private static final boolean    debug = false;
    
    private TreeBuilder(Collection<RingRegion> regions, Iterator<KeyAndVersionChecksum> keyIterator, 
                        int entriesPerNode, long estimatedKeys) {
        this.keyIterator = keyIterator;
        rtBuilders = new TreeMap<>();
        for (RingRegion region : regions) {
            if (debug) {
                System.out.printf("Builder: %s\n", region);
            }
            rtBuilders.put(region.getStart(), new RegionTreeBuilder(region, entriesPerNode, estimateRegionKeys(region, estimatedKeys)));
        }
    }
    
    /**
     * Given several regions, build checksum trees for each region using the keys provided by the 
     * KeyAndVersionChecksum iterator.
     * @param regions
     * @param keyIterator
     * @param entriesPerNode
     * @param estimatedKeys
     * @param creationTimeMillis
     * @param minVersion
     * @param maxVersion
     * @param allowRegionNotFound TODO
     * @return
     */
    public static ChecksumTreeGroup build(Collection<RingRegion> regions, Iterator<KeyAndVersionChecksum> keyIterator, 
            int entriesPerNode, long estimatedKeys, long creationTimeMillis, long minVersion, long maxVersion, 
            boolean allowRegionNotFound) {
        return new ChecksumTreeGroup(creationTimeMillis, minVersion, maxVersion, 
                       new TreeBuilder(regions, keyIterator, entriesPerNode, estimatedKeys).build(allowRegionNotFound));
    }
            
    private static int estimateRegionKeys(RingRegion region, long estimatedKeys) {
        return (int)(region.getRingspaceFraction() * (double)estimatedKeys);
    }
    
    private RegionTreeBuilder getBuilderForKey(DHTKey key, boolean allowRegionNotFound) {
        long    p;
        RegionTreeBuilder   rtb;
        Map.Entry<Long,RegionTreeBuilder>    floorEntry;
        
        p = KeyUtil.keyToCoordinate(key);
        floorEntry = rtBuilders.floorEntry(p);
        if (floorEntry != null) {
            rtb = floorEntry.getValue();
        } else {
            rtb = null;
        }
        if (rtb == null) {
            if (!allowRegionNotFound) {
                System.out.printf("%s\t%d\n", key, p);
                throw new RuntimeException("Can't find builder for coordinate ");
            } else {
                return null;
            }
        }
        if (!rtb.getRegion().contains(p)) {
            if (!allowRegionNotFound) {
                System.out.printf("%s\t%d\t%s\n", key, p, rtb.getRegion());
                throw new RuntimeException("Can't find builder for coordinate ");
            } else {
                return null;
            }
        }
        return rtb;
    }

    private NavigableMap<Long,RegionTreeBuilder> build(boolean allowRegionNotFound) {
        while (keyIterator.hasNext()) {
            KeyAndVersionChecksum    kvc;
            RegionTreeBuilder   rtb;
            
            kvc = keyIterator.next();
            rtb = getBuilderForKey(kvc.getKey(), allowRegionNotFound);
            if (rtb != null) {
                rtb.addChecksum(kvc);
            }
        }
        freezeBuilders();
        return rtBuilders;
    }
    
    private void freezeBuilders() {
        for (RegionTreeBuilder rtb : rtBuilders.values()) {
            rtb.freeze();
        }
    }
    
    @Override
    public String toString() {
        return null;
    }
}
