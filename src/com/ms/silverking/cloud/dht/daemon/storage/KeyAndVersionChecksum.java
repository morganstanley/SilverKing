package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.DHTKeyComparator;
import com.ms.silverking.cloud.dht.common.SimpleKey;

/**
 * Key and version checksum pair for use in convergence.
 */
public class KeyAndVersionChecksum implements Comparable<KeyAndVersionChecksum> {
    private final DHTKey    key;
    private final long      versionChecksum; // may need to expand this for multi-version
    
    public KeyAndVersionChecksum(DHTKey key, long versionChecksum) {
        this.key = new SimpleKey(key);
        this.versionChecksum = versionChecksum;
    }
    
    public DHTKey getKey() {
        return key;
    }

    public long getVersionChecksum() {
        return versionChecksum;
    }
    
    @Override 
    public int hashCode() {
        return key.hashCode() ^ (int)versionChecksum;
    }
    
    @Override
    public boolean equals(Object other) {
        KeyAndVersionChecksum    oKVC;
        
        oKVC = (KeyAndVersionChecksum)other;
        return key.equals(oKVC.key) && (versionChecksum == oKVC.versionChecksum);
    }

    @Override
    public String toString() {
        return key.toString() +" "+ Long.toHexString(versionChecksum); 
    }

    @Override
    public int compareTo(KeyAndVersionChecksum o) {
        int comp;
        
        comp = DHTKeyComparator.dhtKeyComparator.compare(key, o.key);
        if (comp == 0) {
            return Long.compare(versionChecksum, o.versionChecksum);
        } else {
            return comp;
        }
    }
    
    ///////////////////////////////////////////////////////////////////////////
    
    private static final int    mslOffset = 0;
    private static final int    lslOffset = 1;
    private static final int    checksumOffset = 2;
    private static final int    serializedSizeLongs = 3;
    
    public static long[] listToArray(List<KeyAndVersionChecksum> kvcList) {
        long[]  kvcArray;
        int     i;
        
        // For now, we ignore the version checksum as we only support write-once
        // FUTURE - support full bitemporal convergence
        kvcArray = new long[kvcList.size() * serializedSizeLongs];
        i = 0;
        for (KeyAndVersionChecksum kvc : kvcList) {
            kvcArray[i + mslOffset] = kvc.getKey().getMSL();
            kvcArray[i + lslOffset] = kvc.getKey().getLSL();
            kvcArray[i + checksumOffset] = kvc.getVersionChecksum();
            i += serializedSizeLongs;
        }
        return kvcArray;
    }
    
    public static List<KeyAndVersionChecksum> arrayToList(long[] kvcArray) {
        List<KeyAndVersionChecksum> kvcList;
        
        // For now, we ignore the version checksum as we only support write-once
        // FUTURE - support full bitemporal convergence
        kvcList = new ArrayList<>(kvcArray.length / serializedSizeLongs);
        for (int i = 0; i < kvcArray.length; i += serializedSizeLongs) {
            kvcList.add(new KeyAndVersionChecksum(new SimpleKey(kvcArray[i + mslOffset], kvcArray[i + lslOffset]), kvcArray[i + checksumOffset]));
        }
        return kvcList;
    }

    public static int entriesInArray(long[] kvcArray) {
        assert kvcArray.length % serializedSizeLongs == 0;
        return kvcArray.length / serializedSizeLongs;
    }
    
    public static Iterator<KeyAndVersionChecksum> getKVCArrayIterator(long[] kvcArray) {
        return new KVCArrayIterator(kvcArray);
    }
    
    private static class KVCArrayIterator implements Iterator<KeyAndVersionChecksum> {
        private final long[]    kvcArray;
        private int index;
        
        public KVCArrayIterator(long[] kvcArray) {
            this.kvcArray = kvcArray;
        }

        @Override
        public boolean hasNext() {
            return index < kvcArray.length;
        }

        @Override
        public KeyAndVersionChecksum next() {
            KeyAndVersionChecksum   kvc;
            
            kvc = kvcAt(kvcArray, index);
            index += serializedSizeLongs;
            return kvc;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
    
    private static boolean isValidKVCIndex(long[] kvcArray, int index) {
		return index < kvcArray.length && (index % serializedSizeLongs) == 0;
    }
    
    public static KeyAndVersionChecksum kvcAt(long[] kvcArray, int index) {
        assert isValidKVCIndex(kvcArray, index);
        
        return new KeyAndVersionChecksum(new SimpleKey(kvcArray[index + mslOffset], 
                kvcArray[index + lslOffset]), kvcArray[index + checksumOffset]);
    }
    
    /*
     * 
     * the commented out methods are a start of an implementation of a faster pruning algorithm
     * 
    private static int getKVCIndexForCoordinate(long[] kvcArray, long p) {
    	int	size;
    	
    	size = entriesInArray(kvcArray);
    	for (int i = 0; i < size; i++) {
    		KeyAndVersionChecksum	kvc;
    		long	kvcP;
    		
    		kvc = kvcAt(kvcArray, i);
    		kvcP = KeyUtil.keyToCoordinate(kvc.getKey());
    	}
    }
    
    public static long[] getKVCArrayForRegion(long[] kvcArray, RingRegion region) {
    	int	i0;
    	int	i1;
    	int	newArraySize;
    	long[]	newArray;
    	
    	i0 = 0;
    	i1 = 0;
    	newArraySize = i1 - i0 + 1;
    	newArray = new long[newArraySize];
    	System.arraycopy(kvcArray, i0 * serializedSizeLongs, newArray, 0, newArraySize * serializedSizeLongs);
    	return newArray;
    }
    */
}
