package com.ms.silverking.cloud.ring;

import java.util.Comparator;

import com.ms.silverking.cloud.dht.common.DHTKeyComparator;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.daemon.storage.KeyAndVersionChecksum;

/**
 * Orders KeyAndVersionChecksums according to their coordinates with a given region.
 */
public class KeyAndVersionChecksumCoordinateComparator implements Comparator<KeyAndVersionChecksum> {
    private final Comparator<Long>  positionComparator;
    
    public KeyAndVersionChecksumCoordinateComparator(RingRegion region) {
        positionComparator = region.positionComparator();
    }
    
    @Override
    public int compare(KeyAndVersionChecksum kvc0, KeyAndVersionChecksum kvc1) {
        int result;
        
        result = positionComparator.compare(KeyUtil.keyToCoordinate(kvc0.getKey()), 
                                          KeyUtil.keyToCoordinate(kvc1.getKey()));
        if (result == 0) {
            result = DHTKeyComparator.dhtKeyComparator.compare(kvc0.getKey(), kvc1.getKey());
            if (result == 0) {
                result = Long.compare(kvc0.getVersionChecksum(), kvc1.getVersionChecksum());
                return result;
            } else {
                return result;
            }
        } else {
            return result;
        }
    }
}
