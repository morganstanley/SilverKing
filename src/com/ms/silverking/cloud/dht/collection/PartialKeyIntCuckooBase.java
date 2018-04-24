package com.ms.silverking.cloud.dht.collection;

import java.util.Iterator;

import com.ms.silverking.cloud.dht.common.DHTKey;

/**
 * Maps DHTKeys to offsets within a Segment. Partial keying allows the the hash table
 * overhead to be greatly reduced since the full key is stored within the segment.
 * 
 * The partial key hash table storage is supplied by concrete classes.
 */
public abstract class PartialKeyIntCuckooBase extends CuckooBase implements Iterable<DHTKeyIntEntry> {
    private final ExternalStore externalStore;
    
    protected final PKIntSubTableBase[]  subTables;
    
    
    protected static final long   emptyEntry = (long)empty << offsetIndexShift;
    
    private static final boolean    debug = false;
    private static final boolean    debugCycle = false;
    private static final boolean    sanityCheck = false;

    // entry - key/value entry
    // bucket - group of entries
    // bucketSize - entriesPerBucket
    
    protected PartialKeyIntCuckooBase(WritableCuckooConfig cuckooConfig, 
                                   ExternalStore externalStore,
                                   PKIntSubTableBase[] subTables) {        
        super(cuckooConfig);
        this.subTables = subTables;
        this.externalStore = externalStore;
        setSubTables(subTables);
    }
    
    protected ExternalStore getExternalStore() {
        return externalStore;
    }
    
    /*
    public int getProbableValue(DHTKey key) {
        long    msl;
        long    lsl;
        
        msl = key.getMSL();
        lsl = key.getLSL();
        for (PKIntSubTableBase subTable : subTables) {
            int rVal;
            
            rVal = subTable.getProbableValue(msl, lsl);
            if (rVal != empty) {
                return rVal;
            }
        }
        return keyNotFound;
    }
    */

    public int get(DHTKey key) {
        long    msl;
        long    lsl;
        
        msl = key.getMSL();
        lsl = key.getLSL();
        for (PKIntSubTableBase subTable : subTables) {
            int rVal;
            
            rVal = subTable.get(msl, lsl);
            if (rVal != empty) {
                return rVal;
            }
        }
        return keyNotFound;
    }
    
    public void put(DHTKey key, int offset) {
        long    lsl;

        if (debug) {
            if (!externalStore.entryMatches(offset, key.getMSL(), key.getLSL())) {
                throw new RuntimeException("put failed sanity check. external store offset doesn't match key");
            }
        }
        lsl = key.getLSL();
        cuckooPut((int)lsl, offset, 0);
    }

    private void cuckooPut(int partialKey, int offset, int attempt) {
        PKIntSubTableBase    subTable;
        boolean         success;

        if (debug) {
            System.out.println("cuckooPut: "+ partialKey +"\t"+ offset +"\t"+ attempt);
        }
        if (attempt > cuckooLimit) {
            throw new TableFullException();
        }
        for (int i = 0; i < subTables.length; i++) {
            int subTableIndex;
            
            subTableIndex = (attempt + i) & subTablesMask;
            subTable = subTables[subTableIndex];
            if (subTable.put(partialKey, offset)) {
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
        success = subTable.put(partialKey, offset);
        if (!success) {
            throw new RuntimeException("panic");
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
    
    /**
     * SegmentPartialKeyCuckoo SubTable. Each SubTable maintains a bucketed 
     * hash table with entries that map partial keys to an offset within the 
     * Segment. At that offset, the full DHTKey can be found.
     * 
     * To support the above mapping, each entry in the hash table contains 
     * two parts: a 32-bit partial key and a 32-bit offset. The format of
     * each entry is [offset, key] which enables key extraction without any
     * shifting. 
     */
    abstract class PKIntSubTableBase extends SubTableBase {
        private final int   a;
        private final int   b;
        private final int   c;
        private final int   d;
        
        private static final int    _singleEntrySize = 1;
                
        PKIntSubTableBase(int numBuckets, int entriesPerBucket, int keyShift) {
            super(numBuckets, entriesPerBucket, _singleEntrySize);
            a = _a[keyShift];
            b = _b[keyShift];
            c = _c[keyShift];
            d = _d[keyShift];
        }
        
        protected abstract void setHT(int index, long offset);
        protected abstract long getHT(int index);
        
        boolean remove(long msl, long lsl) {
            int     bucketIndex;
            
            if (debug) {
                System.out.printf("get %x:%x\n", msl, lsl);
            }
            bucketIndex = getBucketIndex((int)lsl);
            // now search all entries in this bucket for the given key
            for (int i = 0; i < entriesPerBucket; i++) {
                int entryIndex;
                int rVal;
                
                //entryIndex = ((int)msl + i) & entriesMask;
                entryIndex = ((int)((int)lsl >>> balanceShift) + i) & entriesMask;
                //entryIndex = i;
                rVal = entryMatches(msl, lsl, bucketIndex, entryIndex);
                if (rVal != empty) {
                    setPartialKeyAndOffset(bucketIndex, entryIndex, 0, empty);
                    return true;
                }
            }
            return false;
        }
        
        /*
        public int getProbableValue(long msl, long lsl) {
            int     bucketIndex;
            
            if (debug) {
                System.out.printf("get %x:%x\n", msl, lsl);
            }
            bucketIndex = getBucketIndex((int)lsl);
            // now search all entries in this bucket for the given key
            for (int i = 0; i < entriesPerBucket; i++) {
                int entryIndex;
                int rVal;
                
                //entryIndex = ((int)msl + i) & entriesMask;
                entryIndex = ((int)((int)lsl >>> balanceShift) + i) & entriesMask;
                //entryIndex = i;
                rVal = entryProbablyMatches(msl, lsl, bucketIndex, entryIndex);
                if (rVal != empty) {
                    return rVal;
                }
            }
            return empty;
        }
        */
        
        int get(long msl, long lsl) {
            int     bucketIndex;
            
            if (debug) {
                System.out.printf("get %x:%x\n", msl, lsl);
            }
            bucketIndex = getBucketIndex((int)lsl);
            // now search all entries in this bucket for the given key
            for (int i = 0; i < entriesPerBucket; i++) {
                int entryIndex;
                int rVal;
                
                //entryIndex = ((int)msl + i) & entriesMask;
                entryIndex = ((int)((int)lsl >>> balanceShift) + i) & entriesMask;
                //entryIndex = i;
                rVal = entryMatches(msl, lsl, bucketIndex, entryIndex);
                if (rVal != empty) {
                    return rVal;
                }
            }
            return empty;
        }
        
        boolean put(int partialKey, int offset) {
            int index;
            
            index = getBucketIndex(partialKey);
            for (int i = 0; i < entriesPerBucket; i++) {
                int entryIndex;
                
                //entryIndex = ((int)msl + i) & entriesMask;
                entryIndex = ((int)(partialKey >>> balanceShift) + i) & entriesMask;
                //entryIndex = i;
                if (isEmpty(index, entryIndex)) {
                    setPartialKeyAndOffset(index, entryIndex, partialKey, offset);
                    return true;
                }
            }
            return false;
        }
        
        void vacate(int partialKey, int attempt, int entryIndex) {
            int bucketIndex;
            int htIndex;
            int _partialKey;
            int _offset;
            
            bucketIndex = getBucketIndex(partialKey);
            if (debugCycle || debug) {
                System.out.println(attempt +"\t"+ partialKey +"\t"+ bucketIndex);
            }
            htIndex = getHTEntryIndex(bucketIndex, entryIndex);
            //cuckooPut(buf[baseOffset + mslOffset], buf[baseOffset + lslOffset], getValue(index, bucketIndex), attempt + 1);
            _offset = getOffset(htIndex);
            _partialKey = getPartialKey(htIndex);
            cuckooPut(_partialKey, _offset, attempt + 1);
            //System.out.println("marking as empty: "+ index +" "+ bucketIndex);
            //values[index * entriesPerBucket + bucketIndex] = empty;     
            setHT(htIndex, emptyEntry);
            //setBuf(baseOffset, emptyEntry);
        }

        protected final boolean isEmpty(int bucketIndex, int entryIndex) {
            if (debug) {
                System.out.printf("isEmpty %d,%d %x %x\n", entryIndex, bucketIndex, getOffset(bucketIndex, entryIndex), empty);
            }
            return getOffset(bucketIndex, entryIndex) == empty;
        }
        
        /*
        private int entryProbablyMatches(long msl, long lsl, int bucketIndex, int entryIndex) {
            int    htEntryIndex;
            
            htEntryIndex = getHTEntryIndex(bucketIndex, entryIndex);
            if (debug) {
                System.out.printf("%d,%d\t%d\t%d\n", entryIndex, bucketIndex, lsl, getPartialKey(htEntryIndex));
            }
            if ((int)lsl == getPartialKey(htEntryIndex)) {
                int offset;
                
                offset = getOffset(htEntryIndex);
                if (debug) {
                    System.out.println("probable match");
                }
                return offset;
            } else {
                if (debug) {
                    System.out.println("no match");
                }
                return empty;
            }
        }
        */
        
        private int entryMatches(long msl, long lsl, int bucketIndex, int entryIndex) {
            int    htEntryIndex;
            
            htEntryIndex = getHTEntryIndex(bucketIndex, entryIndex);
            if (debug) {
                System.out.printf("%d,%d\t%d\t%d\n", entryIndex, bucketIndex, lsl, getPartialKey(htEntryIndex));
            }
            if ((int)lsl == getPartialKey(htEntryIndex)) {
                int offset;
                //long    entryMSL;
                //long    entryLSL;
                
                // now we must perform a complete check to see if the full key matches
                offset = getOffset(htEntryIndex);
                if (externalStore.entryMatches(offset, msl, lsl)) {
                    if (debug) {
                        System.out.println("match");
                    }
                    return offset;
                } else {
                    if (debug) {
                        System.out.println("no external match");
                    }
                    return empty;
                }
                /*
                entryMSL = segmentBuffer.getLong(offset);
                entryLSL = segmentBuffer.getLong(offset + NumConversion.BYTES_PER_LONG);
                if (debug) {
                    System.out.printf("entryMatches: %16x:%16x\t%16x:%16x %d %d\n", 
                        msl, lsl, entryMSL, entryLSL, htEntryIndex, offset);
                }
                if (msl == entryMSL && lsl == entryLSL) {
                    return offset;
                } else {
                    return empty;
                }
                */
            } else {
                if (debug) {
                    System.out.println("no match");
                }
                return empty;
            }
        }
        
        int getValue(int bucketIndex, int entryIndex) {
            return getOffset(bucketIndex, entryIndex);
        }
        
        long getMSL(int bucketIndex, int entryIndex) {
            return externalStore.getMSL(getOffset(bucketIndex, entryIndex));
        }
        
        long getLSL(int bucketIndex, int entryIndex) {
            return externalStore.getLSL(getOffset(bucketIndex, entryIndex));
        }
        
        private int getPartialKey(int bucketIndex, int entryIndex) {
            int htIndex;
            
            htIndex = getHTEntryIndex(bucketIndex, entryIndex);
            return (int)getHT(htIndex);
        }
        
        private int getPartialKey(int htIndex) {
            //System.out.println("\t"+ (int)buf[0] +"\t"+ (int)buf[baseOffset]);
            return (int)getHT(htIndex);
            //return (int)getBuf(htIndex);
        }
        
        private int getOffset(int htIndex) {
            return (int)(getHT(htIndex) >>> offsetIndexShift);
            //return (int)htBuf.get(htIndex);
        }
        
        private int getOffset(int bucketIndex, int entryIndex) {
            return getOffset(getHTEntryIndex(bucketIndex, entryIndex));
        }
        private void setPartialKeyAndOffset(int bucketIndex, int entryIndex, int partialKey, int offset) {
            int baseOffset;
            
            baseOffset = getHTEntryIndex(bucketIndex, entryIndex); 
            setPartialKeyAndOffset(baseOffset, partialKey, offset);
        }
        
        private void setPartialKeyAndOffset(int htIndex, int partialKey, int offset) {
            setHT(htIndex, ((long)offset << offsetIndexShift) | ((long)partialKey & 0xffffffffL));
            //setBuf(htIndex, ((long)offset << offsetIndexShift) | ((long)partialKey & 0xffffffffL));
            //System.out.printf("%x\t%x\t%x\t::\t", baseOffset, partialKey, vtIndex);
            //System.out.printf("%d %d : %x %d\n", baseOffset, vtIndex, buf[baseOffset], getVTIndex(baseOffset));
            if (sanityCheck && offset != getOffset(htIndex)) {
                throw new RuntimeException("panic");
            }
        }
        
        /**
         * Given a partialKey, compute the bucketIndex
         * @param partialKey
         * @return
         */
        protected int getBucketIndex(int partialKey) {
            int h;
            
            h = partialKey;
            h ^= (h >>> a) ^ (h >>> b);
            h ^= (h >>> c) ^ (h >>> d);
            //return (partialKey >>> keyShift) & bitMask;
            return h & bitMask;
        }
    }
    
    @Override
    public Iterator<DHTKeyIntEntry> iterator() {
        return new CuckooIterator();
    }
    
    /**
     * Iterate through PartialKeyIntCuckoo entries. Each entry will contain a key and the offset
     * within the segment of that key or the index of the list if multiple values are present for
     * that key.
     */
    class CuckooIterator extends CuckooIteratorBase implements Iterator<DHTKeyIntEntry> {
        CuckooIterator() {
            super();
        }
        
        @Override
        public DHTKeyIntEntry next() {
            DHTKeyIntEntry    mapEntry;

            // precondition: moveToNonEmpty() has been called
            mapEntry = new DHTKeyIntEntry(
                            subTables[subTable].getMSL(bucket, entry),
                            subTables[subTable].getLSL(bucket, entry),
                            subTables[subTable].getValue(bucket, entry));
            moveToNonEmpty();
            return mapEntry;
        }
        
        boolean curIsEmpty() {
            return subTables[subTable].getValue(bucket, entry) == empty;
        }
    }
}
