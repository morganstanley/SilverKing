package com.ms.silverking.cloud.dht.collection.oldpkc;

import java.util.HashMap;
import java.util.Map;

import com.ms.silverking.numeric.NumConversion;
import com.ms.gpcg.numeric.NumUtil;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

/**
 * 
 */
public class TestMap {
    private final int           capacity;
    private final int           bufferCapacity;
    private final long[]        buf;
    private final int           keyShift;
    private final int           probeLimit;
    
    private static final int   mslOffset = 0;
    private static final int   lslOffset = 1;
    private static final int   versionOffset = 2;
    private static final int   offsetOffset = 3;
    private static final int   entrySize = 4;
    
    private static final long   empty = Long.MIN_VALUE;
    
    public TestMap(long[] buf, int probeLimit) {
        this.buf = buf;
        this.probeLimit = probeLimit;
        bufferCapacity = buf.length;
        if (Integer.bitCount(bufferCapacity) != 1) {
            throw new RuntimeException("Supplied buffer must be a perfect power of 2");
        }
        if (bufferCapacity % entrySize != 0) {
            throw new RuntimeException("Supplied buffer must be divisible by "+ entrySize);
        }
        this.capacity = bufferCapacity / entrySize;
        keyShift = NumUtil.log2OfPerfectPower(bufferCapacity) - 1;
    }
    
    public void clear() {
        for (int i = 0; i < bufferCapacity; i++) {
            buf[i] = empty;
        }
    }
    
    public long get(DHTKey key, long version) {
        int index;
        
        index = getIndex(key, version);
        return getOffset(index);
    }
    
    public void put(DHTKey key, long version, long offset) {
        int index;
        
        index = getIndex(key, version);
        putOffset(index, key, version, offset);
    }
    
    private int hash(DHTKey key, long version) {
        return (int)(key.getLSL() ^ version);
    }
    
    private int getIndex(DHTKey key, long version) {
        int hash;
        
        //System.out.println("getIndex: "+ entryString(key, version));
        hash = hash(key, version);
        for (int i = 0; i < probeLimit; i++) {
            int index;
            
            index = Math.abs(Integer.rotateLeft(hash >> keyShift, i)) % capacity;
            //System.out.println("\ti: "+ i +"\tindex: "+ index +"\toffset: "+ getOffset(i));
            if (getOffset(i) == empty) {
                //System.out.println("found empty: "+ i);
                return index;
            } else {
                if (entryMatches(key, version, index)) {
                    //System.out.println("found match: "+ i);
                    return index;
                }
            }
        }
        throw new RuntimeException("Failed to find index");
    }
    
    private void displayEntry(int index) {
        System.out.println(index +"\t"+ entryString(index));
    }
    
    private String entryString(DHTKey key, long version) {
        return ""+ key.getMSL()
                +"\t"+ key.getLSL()
                +"\t"+ version;
    }
    
    private String entryString(int index) {
        int    baseOffset;
        
        baseOffset = entrySize * index;
        return ""+ buf[baseOffset + mslOffset]
                +"\t"+ buf[baseOffset + lslOffset]
                +"\t"+ buf[baseOffset + versionOffset];
    }
    
    private boolean entryMatches(DHTKey key, long version, int index) {
        int    baseOffset;
        
        displayEntry(index);
        baseOffset = entrySize * index;
        //System.out.println("\t"+ index +"\t"+ baseOffset);
        return buf[baseOffset + mslOffset] == key.getMSL()
                && buf[baseOffset + lslOffset] == key.getLSL()
                && buf[baseOffset + versionOffset] == version;
    }
    
    private long getOffset(int index) {
        int    baseOffset;
        
        baseOffset = entrySize * index;
        return buf[baseOffset + offsetOffset];
    }
    
    private void putOffset(int index, DHTKey key, long version, long offset) {
        int baseOffset;
        
        baseOffset = entrySize * index; 
        buf[baseOffset + mslOffset] = key.getMSL();
        //System.out.println("baseOffset: "+ baseOffset +"\tmsl: "+ buf[entrySize * index + mslOffset));
        buf[baseOffset + lslOffset] = key.getLSL();
        buf[baseOffset + versionOffset] = version;
        buf[baseOffset + offsetOffset] = offset;
    }
    
    //////
    
    public static void main(String[] args) {
        try {
            OldMD5KeyDigest        digest;
            TestMap  map;
            Stopwatch   sw;
            Map<DHTKey,Long>    jmap;
            DHTKey[]            key;
            int                 inner;
            int                 outer;
            double              usPerOp;
            long                ops;
               
            jmap = new HashMap<>(32768);
            map = new TestMap(new long[entrySize * 32768], 16);
            map.clear();
            
            digest = new OldMD5KeyDigest();
            key = new DHTKey[10000];
            for (int i = 0; i < 10000; i++) {
                key[i] = digest.computeKey((""+ i).getBytes());
            }
            
            outer = 1;
            inner = 10000;
            sw = new SimpleStopwatch();
            for (int j = 0; j < outer; j++) {
                for (int i = 0; i < inner; i++) {
                    int     version;
                    int     offset;
                    
                    version = 0;
                    offset = i;
                    map.put(key[i], version, offset);
                    //jmap.put(key[i], (long)offset);
                    //System.out.println(i +"\t"+ map.get(key, version));
                }
            }
            sw.stop();
            
            outer = 100;
            ops = inner * outer;
            usPerOp = sw.getElapsedSeconds() / (double)(ops / 1000000);
            System.out.println(sw);
            System.out.println(usPerOp);
            
            sw = new SimpleStopwatch();
            for (int j = 0; j < outer; j++) {
                for (int i = 0; i < inner; i++) {
                    int     version;
                    int     offset;
                    
                    version = 0;
                    offset = i;
                    map.get(key[i], version);
                    //jmap.put(key[i], (long)offset);
                    //System.out.println(i +"\t"+ map.get(key, version));
                }
            }
            sw.stop();
            
            ops = inner * outer;
            usPerOp = sw.getElapsedSeconds() / (double)(ops / 1000000);
            System.out.println("\n"+ sw);
            System.out.println(usPerOp);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
