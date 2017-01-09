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
public class ArrayCuckoo {
    private final int           numSubTables;
    private final int           cuckooLimit;
    private final SubTable[]    subTables;
        
    private static final long   empty = Long.MIN_VALUE;
    private static final int[]  extraShiftPerTable = {0, 0, 32, 32, 16};
    
    private static final boolean    debug = false;
    private static final boolean    debugCycle = false;
    
    public ArrayCuckoo(int numSubTables, int bufferCapacity, int cuckooLimit) {
        int subTableCapacity;
        
        if (numSubTables < 2) {
            throw new RuntimeException("numSubTables must be >= 2");
        }
        this.numSubTables = numSubTables;
        this.cuckooLimit = cuckooLimit;
        subTableCapacity = bufferCapacity / numSubTables;
        subTables = new SubTable[numSubTables];
        for (int i = 0; i < subTables.length; i++) {
            subTables[i] = new SubTable(subTableCapacity, extraShiftPerTable[numSubTables] * i);
        }
    }
    
    public void clear() {
        for (SubTable subTable : subTables) {
            subTable.clear();
        }
    }
    
    /*
    public long get(DHTKey key) {
        long    offset;
        
        offset = subTables[0].get(key);
        if (offset == empty) {
            return subTables[1].get(key);
        } else {
            return empty;
        }
    }
    */

    public long get(DHTKey key) {
        long    msl;
        long    lsl;
        
        msl = key.getMSL();
        lsl = key.getLSL();
        for (SubTable subTable : subTables) {
            long    rVal;
            
            rVal = subTable.get(msl,lsl);
            if (rVal != empty) {
                return rVal;
            }
        }
        return empty;
    }
    
    public void put(DHTKey key, long offset) {
        long[]  dummy;
        
        dummy = new long[3];
        dummy[0] = key.getMSL();
        dummy[1] = key.getLSL();
        dummy[2] = offset;
        cuckooPut(dummy, 0, 0);
    }

    private void cuckooPut(long[] array, int baseOffset, int attempt) {
        SubTable    subTable;
        boolean     success;
        long        msl;
        long        lsl;
        long        offset;
        
        msl = array[baseOffset + 0];
        lsl = array[baseOffset + 1];
        offset = array[baseOffset + 2];
        if (attempt > cuckooLimit) {
            throw new RuntimeException("cuckoo limit exceeded");
        }
        for (int i = 0; i < subTables.length; i++) {
            int subTableIndex;
            
            subTableIndex = (attempt + i) % subTables.length;
            subTable = subTables[subTableIndex];
            if (subTable.put(msl, lsl, offset)) {
                if (debug) {
                    System.out.println("success: "+ msl +":"+ lsl);
                }
                return;
            }
        }
        subTable = subTables[attempt % subTables.length];
        if (debug) {
            System.out.println("vacate: "+ msl +":"+ lsl +"\t"+ attempt +"\t"+ (attempt % subTables.length));
        }
        subTable.vacate(msl, lsl, attempt);
        success = subTable.put(msl, lsl, offset);
        if (!success) {
            throw new RuntimeException("panic");
        }
    }
    
    public void displaySizes() {
        for (int i = 0; i < subTables.length; i++) {
            System.out.println(i +"\t"+ subTables[i].size());
        }
    }
    
    class SubTable {
        private final int           capacity;
        private final int           bufferCapacity;
        private final long[]        buf;
        private final int           keyShift;
        private final int           bitMask;
        
        private static final int   mslOffset = 0;
        private static final int   lslOffset = 1;
        private static final int   offsetOffset = 2;
        private static final int   entrySize = 4;
                
        SubTable(int capacity, int extraShift) {
            this.bufferCapacity = capacity * entrySize;
            if (Integer.bitCount(bufferCapacity) != 1) {
                throw new RuntimeException("Supplied buffer must be a perfect power of 2");
            }
            if (bufferCapacity % entrySize != 0) {
                throw new RuntimeException("Supplied buffer must be divisible by "+ entrySize);
            }
            this.buf = new long[bufferCapacity];
            this.capacity = capacity;
            //keyShift = NumUtil.log2OfPerfectPower(bufferCapacity) - 1 + extraShift;
            keyShift = extraShift;
            bitMask = 0xffffffff >>> (32 - (NumUtil.log2OfPerfectPower(capacity)));
            //System.out.printf("%d\t%x\n", NumUtil.log2OfPerfectPower(bufferCapacity) - 1, bitMask);
        }
        
        void clear() {
            for (int i = 0; i < bufferCapacity; i++) {
                buf[i] = empty;
            }
        }
        
        int size() {
            int size;
            
            size = 0;
            for (int i = 0; i < capacity; i++) {
                size += buf[i * entrySize + offsetOffset] == empty ? 0 : 1;
            }
            return size;
        }
        
        /*
        long _get(long msl, long lsl) {
            int     index;
            
            index = getIndex(lsl);
            if (entryMatches(msl, lsl, index)) {
                return getOffset(index);
            } else {
                return empty;
            }
        }
        */
        
        long get(long msl, long lsl) {
            int index;
            int baseOffset;
            
            index = (int)(lsl >> keyShift) & bitMask;
            baseOffset = entrySize * index;
            if (buf[baseOffset + mslOffset] == msl
                    && buf[baseOffset + lslOffset] == lsl) {
                return buf[baseOffset + offsetOffset];
            } else {
                return empty;
            }
        }
        
        boolean put(long msl, long lsl, long offset) {
            int index;
            
            index = getIndex(lsl);
            if (!isEmpty(index)) {
                return false;
            } else {
                putOffset(index, msl, lsl, offset);
                return true;
            }
        }
        
        public void vacate(long msl, long lsl, int attempt) {
            int index;
            
            index = getIndex(lsl);
            if (debugCycle || debug) {
                System.out.println(attempt +"\t"+ lsl +"\t"+ index);
            }
            cuckooPut(buf, index * entrySize, attempt + 1);
            buf[index * entrySize + offsetOffset] = empty;
        }
        
        private int getIndex(long lsl) {
            //return Math.abs((int)(lsl >> keyShift)) % capacity;
            if (debug) {
                System.out.printf("%x\t%x\t%x\t%d\n", lsl, bitMask, ((int)(lsl >> keyShift) & bitMask), ((int)(lsl >> keyShift) & bitMask));
            }
            return (int)(lsl >> keyShift) & bitMask;
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
                    +"\t"+ buf[baseOffset + offsetOffset];
        }
        
        private boolean isEmpty(int index) {
            int    baseOffset;
            
            baseOffset = entrySize * index;
            return buf[baseOffset + offsetOffset] == empty;
        }
        
        private boolean entryMatches(DHTKey key, int index) {
            int    baseOffset;
            
            //displayEntry(index);
            baseOffset = entrySize * index;
            //System.out.println("\t"+ index +"\t"+ baseOffset);
            return buf[baseOffset + mslOffset] == key.getMSL()
                    && buf[baseOffset + lslOffset] == key.getLSL();
        }
        
        private boolean entryMatches(long msl, long lsl, int index) {
            int    baseOffset;
            
            //displayEntry(index);
            baseOffset = entrySize * index;
            //System.out.println("\t"+ index +"\t"+ baseOffset);
            return buf[baseOffset + mslOffset] == msl
                    && buf[baseOffset + lslOffset] == lsl;
        }
        
        private long getOffset(int index) {
            int    baseOffset;
            
            baseOffset = entrySize * index;
            return buf[baseOffset + offsetOffset];
        }
        
        private void putOffset(int index, long msl, long lsl, long offset) {
            int baseOffset;
            
            baseOffset = entrySize * index; 
            buf[baseOffset + mslOffset] = msl;
            //System.out.println("baseOffset: "+ baseOffset +"\tmsl: "+ buf[entrySize * index + mslOffset));
            buf[baseOffset + lslOffset] = lsl;
            buf[baseOffset + offsetOffset] = offset;
        }
    }
    
    //////
    
    public static void main(String[] args) {
        try {
            OldMD5KeyDigest        digest;
            ArrayCuckoo  map;
            Stopwatch   sw;
            Map<DHTKey,Long>    jmap;
            DHTKey[]            key;
            int                 inner;
            int                 outer;
            double              usPerOp;
            long                ops;
            int                 numSubTables;
            int                 totalEntries;
            int                 entriesPerSubTable;
            int                 load;
            double              loadFactor;
               
            numSubTables = 2;
            entriesPerSubTable = 16384;
            load = (int)((double)(entriesPerSubTable * numSubTables) * .4);
            
            totalEntries = numSubTables * entriesPerSubTable;
            loadFactor = (double)load / (double)totalEntries;
            System.out.println("loadFactor: "+ loadFactor);
            
            jmap = new HashMap<>(totalEntries);
            map = new ArrayCuckoo(numSubTables, totalEntries, 64);
            map.clear();
            
            digest = new OldMD5KeyDigest();
            key = new DHTKey[load];
            for (int i = 0; i < load; i++) {
                key[i] = digest.computeKey((""+ i).getBytes());
            }
            
            outer = 1;
            inner = load;
            sw = new SimpleStopwatch();
            for (int j = 0; j < outer; j++) {
                for (int i = 0; i < inner; i++) {
                    int     offset;
                    
                    offset = i;
                    if (debugCycle) {
                        System.out.println(key[i]);
                    }
                    map.put(key[i], offset);
                    //jmap.put(key[i], (long)offset);
                    //System.out.println(i +"\t"+ map.get(key, version));
                }
            }
            sw.stop();
            
            ops = inner * outer;
            usPerOp = sw.getElapsedSeconds() / ((double)ops / 1000000.0);
            System.out.println(sw);
            System.out.println(usPerOp);
            
            outer = 10000;
            sw = new SimpleStopwatch();
            for (int j = 0; j < outer; j++) {
                for (int i = 0; i < inner; i++) {
                    long     offset;
                    
                    offset = map.get(key[i]);
                    if (offset != i) {
                        System.out.println(offset +" != "+ i);
                    }
                    //jmap.put(key[i], (long)offset);
                    //System.out.println(i +"\t"+ map.get(key[i]));
                }
            }
            sw.stop();
            
            ops = inner * outer;
            usPerOp = sw.getElapsedSeconds() / ((double)ops / 1000000.0);
            System.out.println("\n"+ sw);
            System.out.println(usPerOp);
            System.out.println();
            map.displaySizes();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
