package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.cloud.dht.collection.PKCReaderWriter;
import com.ms.silverking.cloud.dht.collection.SVTReaderWriter;
import com.ms.silverking.cloud.dht.collection.WritableCuckooConfig;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.util.PropertiesHelper;

/**
 * Central repository for storage subsystem configuration parameters. 
 */
class StoreConfiguration {
    /*
    // development
    public static final int fileSegmentCacheCapacity = 5;
    //public static final int ramSegmentSizeBytes = 1 * 1024;
    public static final int ramSegmentSizeBytes = 4 * 1024;
    public static final int ramSegmentPKCSubTables = 4;
    public static final int ramSegmentPKCEntriesPerBucket = 4;
    //public static final int ramSegmentPKCTotalEntries = 1024;
    public static final int ramSegmentPKCTotalEntries = 32;
    public static final int ramSegmentPKCCuckooLimit = 128;
    // long for the partial key 2 longs for the full ket in the vt + the value in the vt
    public static final int indexSizeBytes = // verify
            PKCReaderWriter.overheadBytes + SVTReaderWriter.overheadBytes + 
            ramSegmentPKCTotalEntries * (3 * NumConversion.BYTES_PER_LONG + NumConversion.BYTES_PER_INT);
    
    public static final int fileSegmentPKCSubTables = 4;
    public static final int fileSegmentPKCEntriesPerBucket = 4;
    public static final int fileSegmentPKCTotalEntries = 1024;
    public static final int dataSegmentSizeBytes = 4 * 1024;
    */
    
    /**/
    // production
    
    static {
    	fileSegmentCacheCapacity = PropertiesHelper.systemHelper.getInt(DHTConstants.fileSegmentCacheCapacityProperty, DHTConstants.defaultFileSegmentCacheCapacity);
    }
    
    public static final int fileSegmentCacheCapacity;
    public static final int ramSegmentSizeBytes = 64 * 1024 * 1024;
    public static final int ramSegmentPKCSubTables = 4;
    public static final int ramSegmentPKCEntriesPerBucket = 4;
    //public static final int ramSegmentPKCTotalEntries = 1024;
    public static final int ramSegmentPKCTotalEntries = 512 * 1024; // FIXME temp
    public static final int ramSegmentPKCCuckooLimit = 128;
    // long for the partial key 2 longs for the full ket in the vt + the value in the vt
    public static final int indexSizeBytes = // verify
            PKCReaderWriter.overheadBytes + SVTReaderWriter.overheadBytes + 
            ramSegmentPKCTotalEntries * (3 * NumConversion.BYTES_PER_LONG + NumConversion.BYTES_PER_INT);

    public static final WritableCuckooConfig   ramInitialCuckooConfig = 
            new WritableCuckooConfig(ramSegmentPKCTotalEntries, ramSegmentPKCSubTables, 
                                     ramSegmentPKCEntriesPerBucket, ramSegmentPKCCuckooLimit);
    
    public static final int fileSegmentPKCSubTables = 4;
    public static final int fileSegmentPKCEntriesPerBucket = 4;
    public static final int fileSegmentPKCTotalEntries = 1024;
    public static final int fileSegmentPKCCuckooLimit = 128;
    
    public static final WritableCuckooConfig   fileInitialCuckooConfig = 
            new WritableCuckooConfig(fileSegmentPKCTotalEntries, fileSegmentPKCSubTables, 
                                     fileSegmentPKCEntriesPerBucket, fileSegmentPKCCuckooLimit);
    
    public static final int dataSegmentSizeBytes = 64 * 1024 * 1024;
}
