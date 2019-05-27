package com.ms.silverking.cloud.dht.collection;

public class WritableCuckooConfig extends CuckooConfig {
    private final int    cuckooLimit;
    
    public WritableCuckooConfig(int totalEntries, int numSubTables, int entriesPerBucket, int cuckooLimit) {
        super(totalEntries, numSubTables, entriesPerBucket);
        this.cuckooLimit = cuckooLimit;
    }
    
    public WritableCuckooConfig(CuckooConfig cuckooConfig, int cuckooLimit) {
        this(cuckooConfig.getTotalEntries(), cuckooConfig.getNumSubTables(), 
                cuckooConfig.getEntriesPerBucket(), cuckooLimit);
    }

    public static WritableCuckooConfig nonWritableConfig(int totalEntries, int numSubTables, 
                                                        int entriesPerBucket) {
        return new WritableCuckooConfig(totalEntries, numSubTables, entriesPerBucket, -1);
    }
    
    public static WritableCuckooConfig nonWritableConfig(CuckooConfig cuckooConfig) {
        return nonWritableConfig(cuckooConfig.getTotalEntries(), 
                        cuckooConfig.getNumSubTables(), cuckooConfig.getEntriesPerBucket());
    }
    
    public WritableCuckooConfig doubleEntries() {
        return new WritableCuckooConfig(getTotalEntries() * 2, getNumSubTables(), 
                                        getEntriesPerBucket(), getCuckooLimit());
    }
    
    public WritableCuckooConfig newTotalEntries(int totalEntries) {
        return new WritableCuckooConfig(totalEntries, getNumSubTables(), 
                getEntriesPerBucket(), getCuckooLimit());
    }
    
    public int getCuckooLimit() {
        return cuckooLimit;
    }
        
    @Override
    public String toString() {
        return super.toString() +":"+ cuckooLimit;
    }
}
