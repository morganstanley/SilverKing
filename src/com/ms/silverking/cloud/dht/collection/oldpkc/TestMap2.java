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
public class TestMap2 {
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
    
    public TestMap2(long[] buf, int probeLimit) {
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
    
    public long get(DHTKey key) {
        int index;
        
        index = getIndex(key);
        return getOffset(index);
    }
    
    public void put(DHTKey key, long version, long offset) {
        int index;
        
        index = getIndex(key);
        putOffset(index, key, version, offset);
    }
    
    private int hash(DHTKey key) {
        return (int)(key.getLSL());
    }
    
    private int getIndex(DHTKey key) {
        int hash;
        
        //System.out.println("getIndex: "+ entryString(key, version));
        hash = hash(key);
        for (int i = 0; i < probeLimit; i++) {
            int index;
            
            index = Math.abs(Integer.rotateLeft(hash >> keyShift, i)) % capacity;
            //System.out.println("\ti: "+ i +"\tindex: "+ index +"\toffset: "+ getOffset(i));
            if (getOffset(i) == empty) {
                //System.out.println("found empty: "+ i);
                return index;
            } else {
                if (entryMatches(key, index)) {
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
    
    private boolean entryMatches(DHTKey key, int index) {
        int    baseOffset;
        
        displayEntry(index);
        baseOffset = entrySize * index;
        //System.out.println("\t"+ index +"\t"+ baseOffset);
        return buf[baseOffset + mslOffset] == key.getMSL()
                && buf[baseOffset + lslOffset] == key.getLSL();
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
            TestMap2  map;
            Stopwatch   sw;
            Map<DHTKey,Long>    jmap;
            DHTKey[]            key;
            int                 inner;
            int                 outer;
            double              usPerOp;
            long                ops;
               
            //jmap = new HashMap<>(32768);
            jmap = new HashMap<>(20000, 10000.0f);
            map = new TestMap2(new long[entrySize * 32768], 16);
            map.clear();
            
            digest = new OldMD5KeyDigest();
            key = new DHTKey[10000];
            for (int i = 0; i < 10000; i++) {
                key[i] = digest.computeKey((""+ i).getBytes());
            }
            
            outer = 10000;
            inner = 10000;
            sw = new SimpleStopwatch();
            for (int j = 0; j < outer; j++) {
                for (int i = 0; i < inner; i++) {
                    int     version;
                    int     offset;
                    
                    version = 0;
                    offset = i;
                    //map.put(key[i], version, offset);
                    jmap.put(key[i], (long)offset);
                    //System.out.println(i +"\t"+ map.get(key, version));
                }
            }
            sw.stop();
            
            ops = inner * outer;
            usPerOp = sw.getElapsedSeconds() / (double)(ops / 1000000);
            System.out.println(sw);
            System.out.println(usPerOp);
            
            outer = 10000;
            sw = new SimpleStopwatch();
            for (int j = 0; j < outer; j++) {
                for (int i = 0; i < inner; i++) {
                    int     version;
                    long    offset;
                    
                    version = 0;
                    //offset = map.get(key[i]);
                    offset = jmap.get(key[i]);
                    if (offset != i) {
                        System.out.println(offset +" != "+ i);
                    }
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
