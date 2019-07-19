package com.ms.silverking.cloud.dht.collection;

import com.ms.silverking.cloud.dht.common.DHTKey;


/**
 * Stores the hashtable in RAM. Full keys still stored in ExternalStore.
 */
public class HybridPartialKeyCuckoo extends PartialKeyIntCuckooBase {
    private final long[]  ht;
    
    protected HybridPartialKeyCuckoo(WritableCuckooConfig cuckooConfig, ExternalStore externalStore,
            long[] ht, boolean initialize) {
        super(cuckooConfig, externalStore, new SubTable[cuckooConfig.getNumSubTables()]);
        this.ht = ht;
        for (int i = 0; i < subTables.length; i++) {
            subTables[i] = new SubTable(ht, subTableBuckets * entriesPerBucket * i, subTableBuckets, cuckooConfig.getEntriesPerBucket(), i);
        }
        if (initialize) {
            initialize();
        }
    }
    
    protected HybridPartialKeyCuckoo(WritableCuckooConfig cuckooConfig, ExternalStore externalStore,
            long[] ht) {
        this(cuckooConfig, externalStore, ht, true);
    }
    
    public HybridPartialKeyCuckoo(WritableCuckooConfig cuckooConfig, 
                                  ExternalStore externalStore) {
        this(cuckooConfig, externalStore, new long[cuckooConfig.getTotalEntries()]);
    }
    
    public long[] getHashTableArray() {
        return ht;
    }
    
    class SubTable extends PKIntSubTableBase {
        private final long[]    ht;
        private final int       htOffset;
        
        SubTable(long[] ht, int htOffset, int numBuckets, int entriesPerBucket, int keyShift) {
            super(numBuckets, entriesPerBucket, keyShift);
            this.ht = ht;
            this.htOffset = htOffset;
        }
        
        void clear() {
            for (int i = 0; i < bufferSizeLongs; i++) {
                //System.out.printf("%d\t%x\n", i, emptyEntry);
                ht[htOffset + i] = emptyEntry;
                //setBuf(i, emptyEntry);
            }
        }

        @Override
        protected void setHT(int index, long offset) {
            ht[htOffset + index] = offset;
        }

        @Override
        protected long getHT(int index) {
            //System.out.printf("%d\t%x\n", index, htBuf.get(index));
            return ht[htOffset + index];
        }
    }
    

    private static HybridPartialKeyCuckoo rehash(PartialKeyIntCuckooBase oldTable) {
        HybridPartialKeyCuckoo  newTable;
        
        newTable = new HybridPartialKeyCuckoo(oldTable.getConfig().doubleEntries(),
                                              oldTable.getExternalStore());
        try {
            for (DHTKeyIntEntry entry : oldTable) {
                //System.out.println(entry);
                newTable.put(entry.getKey(), entry.getValue());
            }
            return newTable;
        } catch (TableFullException tfe) {
            throw new RuntimeException("Unexpected table full during rehash");
        }
    }
    
    public static HybridPartialKeyCuckoo rehashAndAdd(PartialKeyIntCuckooBase oldTable, DHTKey key, int value) {
        HybridPartialKeyCuckoo  newTable;
        
        newTable = rehash(oldTable);
        try {
            newTable.put(key, value);
            return newTable;
        } catch (TableFullException tfe) {
            throw new RuntimeException("Unexpected table full after rehash");
        }
    }    
    
}
