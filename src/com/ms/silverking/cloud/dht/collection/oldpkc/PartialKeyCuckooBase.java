package com.ms.silverking.cloud.dht.collection.oldpkc;

import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.cloud.dht.collection.WritableCuckooConfig;
import com.ms.silverking.cloud.dht.common.DHTKey;

/**
 * 
 */
public abstract class PartialKeyCuckooBase {
    private final int           totalEntries;
    private final int           numSubTables;
    private final int           entriesPerBucket;
    private final int           cuckooLimit;
    protected SubTableBase[]    subTables;
    private final int           subTablesMask;
    private final int           entriesMask;
    protected final int subTableBuckets;
    
    private final ValueTable    valueTable;
        
    protected static final int   vtIndexShift = 32;
    
    private static final int    empty = ValueTable.noMatch;
    private static final long   emptyEntry = (long)empty << vtIndexShift;
    private static final int[]  base2Masks = {0, 0x0, 0x1, 0, 0x3, 0, 0, 0, 0x7};
    
    private static final boolean    debug = false;
    private static final boolean    debugCycle = false;
    
    // entry - key/value entry
    // bucket - group of entries
    // bucketSize - entriesPerBucket
    
    protected PartialKeyCuckooBase(WritableCuckooConfig cuckooConfig, 
                             ValueTable valueTable, boolean initialize, Object context) {
        this.totalEntries = cuckooConfig.getTotalEntries();
        this.numSubTables = cuckooConfig.getNumSubTables();
        this.entriesPerBucket = cuckooConfig.getEntriesPerBucket();
        this.cuckooLimit = cuckooConfig.getCuckooLimit();
        this.valueTable = valueTable;
        subTableBuckets = cuckooConfig.getNumSubTableBuckets();
        if (debug) {
            System.out.println("totalEntries: "+ totalEntries);
        }
        subTables = new SubTableBase[numSubTables];
        if (initialize) {
            clear();
        }
        subTablesMask = base2Masks[numSubTables];
        entriesMask = base2Masks[entriesPerBucket];
    }
    
    public PartialKeyCuckooBase(WritableCuckooConfig cuckooConfig) {
        this(cuckooConfig, new SimpleValueTable(cuckooConfig.getTotalEntries()), true, null);
    }
    
    //protected abstract SubTableBase createSubTable(int subTableBuckets, int entriesPerBucket, int index, Object context);
    /*
    if (subTableBufs == null) {
        subTables[i] = new SubTable(subTableBuckets, entriesPerBucket, i);
    } else {
        subTables[i] = new SubTable(subTableBuckets, entriesPerBucket, i, subTableBufs[i]);
    }
    */
    
    //public static PartialKeyBase create(int totalEntries, int numSubTables, int entriesPerBucket, 
    //        ValueTable valueTable, long[][] subTableBufs) {
    //    return new PartialKeyBase(numSubTables, entriesPerBucket, totalEntries, 0, valueTable, subTableBufs);
    //}
    

    public int getTotalEntries() {
        return totalEntries;
    }
    
    int getSizeBytes() {
        return totalEntries * NumConversion.BYTES_PER_LONG;
    }
    
    int getNumSubTables() {
        return numSubTables;
    }
    
    int getEntriesPerBucket() {
        return entriesPerBucket;
    }
    
    //long[] getSubTable(int index) {
    //    return subTables[index].buf;
    //}
    
    ValueTable getValueTable() {
        return valueTable;
    }
    
    void clear() {
        valueTable.clear();
        for (SubTableBase subTable : subTables) {
            subTable.clear();
        }
    }
    
    /*
    public int get(DHTKey key) {
        long    msl;
        long    lsl;
        int     rVal;
        // saves 3 ns
        
        msl = key.getMSL();
        lsl = key.getLSL();
        rVal = subTable0.get(msl, lsl);
        if (rVal != empty) {
            return rVal;
        }
        rVal = subTable1.get(msl, lsl);
        if (rVal != empty) {
            return rVal;
        }
        rVal = subTable2.get(msl, lsl);
        if (rVal != empty) {
            return rVal;
        }
        rVal = subTable3.get(msl, lsl);
        if (rVal != empty) {
            return rVal;
        }
        return empty;
    }
    */
    /**/
    public int get(DHTKey key) {
        long    msl;
        long    lsl;
        
        msl = key.getMSL();
        lsl = key.getLSL();
        for (SubTableBase subTable : subTables) {
            int rVal;
            
            rVal = subTable.get(msl, lsl);
            if (rVal != empty) {
                return rVal;
            }
        }
        return empty;
    }
    /**/
    
    public void put(DHTKey key, int value) {
        long    msl;
        long    lsl;
        int     vtIndex;
        
        msl = key.getMSL();
        lsl = key.getLSL();
        vtIndex = valueTable.add(msl, lsl, value);
        cuckooPut((int)lsl, vtIndex, 0);
    }

    private void cuckooPut(int partialKey, int vtIndex, int attempt) {
        SubTableBase    subTable;
        boolean     success;

        if (debug) {
            System.out.println("cuckooPut: "+ partialKey +"\t"+ vtIndex +"\t"+ attempt);
        }
        if (attempt > cuckooLimit) {
            throw new RuntimeException("cuckoo limit exceeded");
        }
        for (int i = 0; i < subTables.length; i++) {
            int subTableIndex;
            
            subTableIndex = (attempt + i) & subTablesMask;
            subTable = subTables[subTableIndex];
            if (subTable.put(partialKey, vtIndex)) {
                if (debug) {
                    System.out.println("success: "+ partialKey);
                }
                return;
            }
        }
        subTable = subTables[attempt & subTablesMask];
        if (debug) {
            System.out.printf("vacate: %x\n", partialKey);
            System.out.println("vacate: "+ partialKey +"\t"+ attempt +"\t"+ (attempt % subTables.length));
        }
        //subTable.vacate(partialKey, attempt, Math.abs(((int)(attempt) & subTablesMask)));
        //subTable.vacate(partialKey, attempt, (int)(partialKey >>> 20) & entriesMask);
        subTable.vacate(partialKey, attempt, 0);
        success = subTable.put(partialKey, vtIndex);
        if (!success) {
            throw new RuntimeException("panic");
        }
    }
    
    public void displaySizes() {
        for (int i = 0; i < subTables.length; i++) {
            System.out.println(i +"\t"+ subTables[i].size());
        }
    }

    /*
    private static final int[]  _a = {0, 20, 21, 19};
    private static final int[]  _b = {0, 12, 13, 12};
    private static final int[]  _c = {0, 7, 8, 6};
    private static final int[]  _d = {0, 4, 5, 3};
    */
    /*
    private static final int[]  _a = {0, 16, 20, 22};
    private static final int[]  _b = {0, 0, 12, 10};
    private static final int[]  _c = {0, 0, 7, 6};
    private static final int[]  _d = {0, 0, 4, 3};
    */
    private static final int[]  _a = {0, 16, 24, 0};
    private static final int[]  _b = {0, 31, 12, 12};
    private static final int[]  _c = {0, 31, 7, 7};
    private static final int[]  _d = {0, 31, 4, 4};
    
    abstract class SubTableBase {
        private final int           bufferSizeLongs;
        private final int           bitMask;
        private final int           entriesPerBucket;
        private final int           bucketSizeLongs;
        private final int           entrySizeLongs;
        private final int           a;
        private final int           b;
        private final int           c;
        private final int           d;
        
        protected static final int   singleEntrySize = 1;
        private static final int   balanceShift = 20;
                
        SubTableBase(int numBuckets, int entriesPerBucket, int keyShift) {
            //System.out.println("numEntries: "+ numBuckets +"\tentriesPerBucket: "+ entriesPerBucket);
            this.entriesPerBucket = entriesPerBucket;
            entrySizeLongs = singleEntrySize;
            bucketSizeLongs = singleEntrySize * entriesPerBucket;
            this.bufferSizeLongs = numBuckets * bucketSizeLongs;
            if (Integer.bitCount(numBuckets) != 1) {
                throw new RuntimeException("Supplied numBuckets must be a perfect power of 2");
            }
            //keyShift = NumUtil.log2OfPerfectPower(bufferCapacity) - 1 + extraShift;
            a = _a[keyShift];
            b = _b[keyShift];
            c = _c[keyShift];
            d = _d[keyShift];
            //bitMask = 0xffffffff >>> (32 - (NumUtil.log2OfPerfectPower(numBuckets)));
            bitMask = numBuckets - 1;
        }
        
        protected abstract void setBuf(int index, long value);
        protected abstract long getBuf(int index);
        
        void clear() {
            for (int i = 0; i < bufferSizeLongs; i++) {
                setBuf(i, emptyEntry);
            }
        }
        
        int size() {
            int size;
            
            size = 0;
            /*
            System.out.println("values.length: "+ values.length);
            for (int i = 0; i < values.length; i++) {
                size += values[i] == empty ? 0 : 1;
            }
            */
            return size;
        }

        private int getBaseOffset(int index, int bucketIndex) {
            return bucketSizeLongs * index + entrySizeLongs * bucketIndex;
            //return (index << 2) + bucketIndex; // saves about 1 ns
        }        
        
        int get(long msl, long lsl) {
            int     index;
            
            if (debug) {
                System.out.printf("get %x:%x\n", msl, lsl);
            }
            index = getIndex((int)lsl);
            for (int i = 0; i < entriesPerBucket; i++) {
                int entryIndex;
                int rVal;
                
                //entryIndex = ((int)msl + i) & entriesMask;
                entryIndex = ((int)((int)lsl >>> balanceShift) + i) & entriesMask;
                //entryIndex = i;
                rVal = entryMatches(msl, lsl, index, entryIndex);
                if (rVal != empty) {
                    return rVal;
                }
            }
            return empty;
        }
        
        boolean put(int partialKey, int vtIndex) {
            int index;
            
            index = getIndex(partialKey);
            for (int i = 0; i < entriesPerBucket; i++) {
                int entryIndex;
                
                //entryIndex = ((int)msl + i) & entriesMask;
                entryIndex = ((int)(partialKey >>> balanceShift) + i) & entriesMask;
                //entryIndex = i;
                if (isEmpty(index, entryIndex)) {
                    setPartialKeyAndVTIndex(index, entryIndex, partialKey, vtIndex);
                    return true;
                }
            }
            return false;
        }
        
        public void vacate(int partialKey, int attempt, int bucketIndex) {
            int index;
            int baseOffset;
            int _partialKey;
            int _vtIndex;
            
            index = getIndex(partialKey);
            if (debugCycle || debug) {
                System.out.println(attempt +"\t"+ partialKey +"\t"+ index);
            }
            baseOffset = getBaseOffset(index, bucketIndex);
            //cuckooPut(buf[baseOffset + mslOffset], buf[baseOffset + lslOffset], getValue(index, bucketIndex), attempt + 1);
            _vtIndex = getVTIndex(baseOffset);
            _partialKey = getPartialKey(baseOffset);
            cuckooPut(_partialKey, _vtIndex, attempt + 1);
            //System.out.println("marking as empty: "+ index +" "+ bucketIndex);
            //values[index * entriesPerBucket + bucketIndex] = empty;            
            setBuf(baseOffset, emptyEntry);
        }
        
        private int getIndex(int partialKey) {
            int h;
            
            h = partialKey;
            h ^= (h >>> a) ^ (h >>> b);
            h ^= (h >>> c) ^ (h >>> d);
            //return (partialKey >>> keyShift) & bitMask;
            return h & bitMask;
        }
        
        private boolean isEmpty(int index, int bucketIndex) {
            return getVTIndex(index, bucketIndex) == empty;
        }
        
        private int entryMatches(long msl, long lsl, int index, int bucketIndex) {
            int    baseOffset;
            
            baseOffset = getBaseOffset(index, bucketIndex);
            //System.out.println((int)lsl +"\t"+ getPartialKey(baseOffset));
            if ((int)lsl == getPartialKey(baseOffset)) {
                int vtIndex;
                
                vtIndex = getVTIndex(baseOffset);
                if (debug) {
                    System.out.printf("entryMatches: %16x:%16x %d %d\n", msl, lsl, baseOffset, vtIndex);
                }
                return valueTable.getValue(vtIndex);
            } else {
                return empty;
            }
        }
        
        private int getPartialKey(int index, int bucketIndex) {
            int baseOffset;
            
            baseOffset = getBaseOffset(index, bucketIndex);
            return (int)getBuf(baseOffset);
        }
        
        private int getPartialKey(int baseOffset) {
            //System.out.println("\t"+ (int)buf[0] +"\t"+ (int)buf[baseOffset]);
            return (int)getBuf(baseOffset);
        }
        
        protected abstract int getVTIndex(int baseOffset);
            //return (int)(buf[baseOffset] >>> vtIndexShift);
        
        private int getVTIndex(int index, int bucketIndex) {
            return getVTIndex(getBaseOffset(index, bucketIndex));
        }
        
        private int getValue(int index, int bucketIndex) {
            //return values[index * entriesPerBucket + bucketIndex];
            return valueTable.getValue(getVTIndex(index, bucketIndex));
        }
        
        private void setPartialKeyAndVTIndex(int index, int entryIndex, int partialKey, int vtIndex) {
            int baseOffset;
            
            baseOffset = getBaseOffset(index, entryIndex); 
            setPartialKeyAndVTIndex(baseOffset, partialKey, vtIndex);
        }
        
        private void setPartialKeyAndVTIndex(int baseOffset, int partialKey, int vtIndex) {
            setBuf(baseOffset, ((long)vtIndex << vtIndexShift) | ((long)partialKey & 0xffffffffL));
            //System.out.printf("%x\t%x\t%x\t::\t", baseOffset, partialKey, vtIndex);
            //System.out.printf("%d %d : %x %d\n", baseOffset, vtIndex, buf[baseOffset], getVTIndex(baseOffset));
            if (vtIndex != getVTIndex(baseOffset)) {
                throw new RuntimeException("panic");
            }
        }
    }
}
