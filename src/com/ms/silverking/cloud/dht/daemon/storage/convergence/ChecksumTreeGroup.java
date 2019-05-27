package com.ms.silverking.cloud.dht.daemon.storage.convergence;

import java.util.Map;
import java.util.NavigableMap;

/**
 * A group of ChecksumTrees for a set of regions and a particular version 
 * as of a particular time. External code should ensure that - in the context
 * of a particular namespace - no two ChecksumTreeGroups have the same
 * creationTimeMillis and versions.
 * 
 *  ChecksumTrees are obtained from RegionTreeBuilders.
 */
public class ChecksumTreeGroup {
    private final long  creationTimeMillis;
    private final long  minVersion;
    private final long  maxVersion;
    private final NavigableMap<Long,RegionTreeBuilder>   regionTreeBuilders;
    
    public ChecksumTreeGroup(long creationTimeMillis, long minVersion, 
            long maxVersion, NavigableMap<Long,RegionTreeBuilder> regionTreeBuilders) {
        this.creationTimeMillis = creationTimeMillis;
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
        this.regionTreeBuilders = regionTreeBuilders;
    }
    
    public void displayRegions() {
    	for (RegionTreeBuilder rtb : regionTreeBuilders.values()) {
    		System.out.printf("%s\n", rtb.getRegion());
    	}
    }
    
    public long getCreationTimeMillis() {
        return creationTimeMillis;
    }

    public long getMinVersion() {
        return minVersion;
    }
    
    public long getMaxVersion() {
        return maxVersion;
    }

    //public Map<Long, RegionTreeBuilder> getRegionTreeBuilders() {
    //    return regionTreeBuilders;
    //}
    
    public ChecksumNode getTreeRoot(long regionStart) {
        RegionTreeBuilder   rtb;
        Map.Entry<Long, RegionTreeBuilder>	floorEntry;
        
        floorEntry = regionTreeBuilders.floorEntry(regionStart);
        rtb = floorEntry != null ? floorEntry.getValue() : null;
        if (rtb != null) {
            return rtb.getRoot();
        } else {
        	/*
            StringBuilder   sb;
            
            sb = new StringBuilder();
            for (Long rs : regionTreeBuilders.keySet()) {
                sb.append(rs +"\n");
            }
            System.out.println(sb);
            */
        	System.err.printf("creationTimeMillis %d minVersion %d maxVersion %d\n", creationTimeMillis, minVersion, maxVersion);
            throw new RuntimeException("Can't find RegionTreeBuilder for: "+ regionStart);
        }
    }
}
