package com.ms.silverking.cloud.dht.collection.oldpkc;

import java.util.HashMap;
import java.util.Map;

import com.ms.gpcg.numeric.NumUtil;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

/**
 * 
 */
public class BucketedArrayCuckoo {
    private final int           numSubTables;
    private final int           entriesPerBucket;
    private final int           cuckooLimit;
    private final SubTable[]    subTables;
    private final int           subTablesMask;
    private final int           entriesMask;
        
    private static final int    empty = Integer.MIN_VALUE;
    private static final int[]  extraShiftPerTable = {-1, -1, 32, -1, 16, -1, -1, -1, 8};
    private static final int[]  base2Masks = {0, 0x0, 0x1, 0, 0x3, 0, 0, 0, 0x7};
    
    private static final boolean    debug = false;
    private static final boolean    debugCycle = false;
    
    // entry - key/value entry
    // bucket - group of entries
    // bucketSize - entriesPerBucket
    
    public BucketedArrayCuckoo(int numSubTables, int entriesPerBucket, int totalEntries, int cuckooLimit) {
        int subTableBuckets;
        
        if (numSubTables < 2) {
            throw new RuntimeException("numSubTables must be >= 2");
        }
        this.numSubTables = numSubTables;
        this.entriesPerBucket = entriesPerBucket;
        this.cuckooLimit = cuckooLimit;
        subTableBuckets = totalEntries / (numSubTables * entriesPerBucket);
        subTables = new SubTable[numSubTables];
        for (int i = 0; i < subTables.length; i++) {
            subTables[i] = new SubTable(subTableBuckets, entriesPerBucket, extraShiftPerTable[numSubTables] * i);
        }
        subTablesMask = base2Masks[numSubTables];
        entriesMask = base2Masks[entriesPerBucket];
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
        int     offset;
        int     stopOffset;
        
        msl = key.getMSL();
        lsl = key.getLSL();
        for (SubTable subTable : subTables) {
            long    rVal;
            
            rVal = subTable.get(msl, lsl);
            if (rVal != empty) {
                return rVal;
            }
        }
        return empty;
    }
    
    /*
    public long get(DHTKey key) {
        long    msl;
        long    lsl;
        int     offset;
        int     stopOffset;
        
        msl = key.getMSL();
        lsl = key.getLSL();
        for (SubTable subTable : subTables) {
            long    rVal;
            
            rVal = subTable.get(msl, lsl);
            if (rVal != empty) {
                return rVal;
            }
        }
        return empty;
    }
    */
    
    public void put(DHTKey key, int value) {
        cuckooPut(key.getMSL(), key.getLSL(), value, 0);
    }

    private void cuckooPut(long msl, long lsl, int value, int attempt) {
        SubTable    subTable;
        boolean     success;
        
        if (attempt > cuckooLimit) {
            throw new RuntimeException("cuckoo limit exceeded");
        }
        for (int i = 0; i < subTables.length; i++) {
            int subTableIndex;
            
            subTableIndex = (attempt + i) & subTablesMask;
            subTable = subTables[subTableIndex];
            if (subTable.put(msl, lsl, value)) {
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
        subTable.vacate(msl, lsl, attempt, Math.abs(((int)(lsl + attempt) % subTable.entriesPerBucket)));
        //subTable.vacate(msl, lsl, attempt, 0/*entriesPerBucket - 1*/);
        success = subTable.put(msl, lsl, value);
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
        private final int           numBuckets;
        private final int           bufferSizeLongs;
        private final long[]        buf;
        private final int[]         values;
        private final int           keyShift;
        private final int           bitMask;
        private final int           entriesPerBucket;
        private final int           bucketSizeLongs;
        private final int           entrySizeLongs;
        
        private static final int   mslOffset = 0;
        private static final int   lslOffset = 1;
        private static final int   singleEntrySize = 2;
                
        SubTable(int numBuckets, int entriesPerBucket, int extraShift) {
            //System.out.println("numEntries: "+ numBuckets +"\tentriesPerBucket: "+ entriesPerBucket);
            this.entriesPerBucket = entriesPerBucket;
            entrySizeLongs = singleEntrySize;
            bucketSizeLongs = singleEntrySize * entriesPerBucket;
            this.bufferSizeLongs = numBuckets * bucketSizeLongs;
            if (Integer.bitCount(numBuckets) != 1) {
                throw new RuntimeException("Supplied numBuckets must be a perfect power of 2");
            }
            buf = new long[bufferSizeLongs];
            values = new int[numBuckets * entriesPerBucket];
            this.numBuckets = numBuckets;
            //keyShift = NumUtil.log2OfPerfectPower(bufferCapacity) - 1 + extraShift;
            keyShift = extraShift;
            bitMask = 0xffffffff >>> (32 - (NumUtil.log2OfPerfectPower(numBuckets)));
            //System.out.printf("%d\t%x\n", NumUtil.log2OfPerfectPower(bufferCapacity) - 1, bitMask);
        }
        
        void clear() {
            for (int i = 0; i < bufferSizeLongs; i++) {
                buf[i] = 0;
            }
            for (int i = 0; i < values.length; i++) {
                values[i] = empty;
            }
        }
        
        int size() {
            int size;
            
            size = 0;
            //System.out.println("values.length: "+ values.length);
            for (int i = 0; i < values.length; i++) {
                size += values[i] == empty ? 0 : 1;
            }
            return size;
        }

        private int getBaseOffset(int index, int bucketIndex) {
            return bucketSizeLongs * index + entrySizeLongs * bucketIndex;
        }        
        
        long get(long msl, long lsl) {
            int     index;
            
            index = getIndex(lsl);
            for (int i = 0; i < entriesPerBucket; i++) {
                int entryIndex;
                
                //if (entryMatches(msl, lsl, index, i)) {
                entryIndex = ((int)msl + i) & entriesMask;
                //entryIndex = i;
                if (entryMatches(msl, lsl, index, entryIndex)) {
                    return getValue(index, entryIndex);
                }
            }
            return empty;
        }
        
        boolean put(long msl, long lsl, int value) {
            int index;
            
            index = getIndex(lsl);
            for (int i = 0; i < entriesPerBucket; i++) {
                int entryIndex;
                
                entryIndex = ((int)msl + i) & entriesMask;
                //entryIndex = i;
                if (isEmpty(index, entryIndex)) {
                    putValue(index, msl, lsl, value, entryIndex);
                    return true;
                }
            }
            return false;
        }
        
        public void vacate(long msl, long lsl, int attempt, int bucketIndex) {
            int index;
            int baseOffset;
            
            index = getIndex(lsl);
            if (debugCycle || debug) {
                System.out.println(attempt +"\t"+ lsl +"\t"+ index);
            }
            baseOffset = getBaseOffset(index, bucketIndex);
            cuckooPut(buf[baseOffset + mslOffset], buf[baseOffset + lslOffset], getValue(index, bucketIndex), attempt + 1);
            //System.out.println("marking as empty: "+ index +" "+ bucketIndex);
            values[index * entriesPerBucket + bucketIndex] = empty;            
            buf[baseOffset + mslOffset] = 0;
            buf[baseOffset + lslOffset] = 0;
        }
        
        private int getIndex(long lsl) {
            //return Math.abs((int)(lsl >> keyShift)) % capacity;
            if (debug) {
                System.out.printf("%x\t%x\t%x\t%d\n", lsl, bitMask, ((int)(lsl >> keyShift) & bitMask), ((int)(lsl >> keyShift) & bitMask));
            }
            return (int)(lsl >>> keyShift) & bitMask;
        }
        
        private void displayEntry(int index, int bucketIndex) {
            System.out.println(index +"\t"+ entryString(index, bucketIndex));
        }
        
        private String entryString(int index, int bucketIndex) {
            int    baseOffset;
            
            baseOffset = getBaseOffset(index, bucketIndex);
            return ""+ buf[baseOffset + mslOffset]
                    +"\t"+ buf[baseOffset + lslOffset]
                    +"\t"+ values[index];
        }
        
        private boolean isEmpty(int index, int bucketIndex) {
            return getValue(index, bucketIndex) == empty;
        }
        
        private boolean entryMatches(long msl, long lsl, int index, int bucketIndex) {
            int    baseOffset;
            
            //displayEntry(index);
            baseOffset = getBaseOffset(index, bucketIndex);
            //System.out.println("\t"+ index +"\t"+ bucketIndex +"\t"+ baseOffset);
            return buf[baseOffset + mslOffset] == msl
                    && buf[baseOffset + lslOffset] == lsl;
        }
        
        private int getValue(int index, int bucketIndex) {
            return values[index * entriesPerBucket + bucketIndex];
        }
        
        private void putValue(int index, long msl, long lsl, int value, int bucketIndex) {
            int baseOffset;
            
            //System.out.println(index +"\t"+ bucketIndex);
            assert bucketIndex < entriesPerBucket;
            baseOffset = getBaseOffset(index, bucketIndex); 
            buf[baseOffset + mslOffset] = msl;
            //System.out.println("baseOffset: "+ baseOffset +"\tmsl: "+ buf[entrySize * index + mslOffset));
            buf[baseOffset + lslOffset] = lsl;
            values[index * entriesPerBucket + bucketIndex] = value;
        }
    }
    
    //////
    
    public static void main(String[] args) {
        try {
            OldMD5KeyDigest        digest;
            BucketedArrayCuckoo  map;
            Stopwatch   sw;
            Map<DHTKey,Long>    jmap;
            DHTKey[]            key;
            int                 inner;
            int                 outer;
            double              usPerOp;
            long                ops;
            int                 numSubTables;
            int                 entriesPerBucket;
            int                 totalEntries;
            int                 entriesPerSubTable;
            int                 load;
            double              loadFactor;
               
            
            //8/bucket, 2tables, .96 generates errors
            totalEntries = 32768;
            loadFactor = Double.parseDouble(args[0]);
            entriesPerBucket = 4;
            numSubTables = 4;
            entriesPerSubTable = totalEntries / (numSubTables * entriesPerBucket);
            //entriesPerSubTable = 16384 / entriesPerBucket;
            //totalEntries = numSubTables * entriesPerSubTable * entriesPerBucket;
            
            load = (int)((double)(totalEntries) * loadFactor);
            
            System.out.println("entriesPerSubTable: "+ entriesPerSubTable +"\ttotalEntries: "+ totalEntries);
            System.out.println("loadFactor: "+ loadFactor +"\tload: "+ load);
            
            jmap = new HashMap<>(totalEntries);
            map = new BucketedArrayCuckoo(numSubTables, entriesPerBucket, totalEntries, 128);
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

            System.out.println("warmup");
            sw = new SimpleStopwatch();
            for (int j = 0; j < 100; j++) {
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
            try {
                Thread.sleep(20);
            } catch (InterruptedException ie) {
            }
            System.out.println("warmup complete");
            
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
