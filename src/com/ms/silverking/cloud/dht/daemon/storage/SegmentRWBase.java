package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;

import com.ms.silverking.numeric.NumConversion;

/**
 * Segment format:
 * <Field>          <size in bytes>
 * format version   2
 * segment type     2
 * segment number   4
 * index offset     4
 * data offset      4
 * 
 * FUTURE - CACHE ALIGN THE PKC
 * 
 */
abstract class SegmentRWBase {
    protected final File    nsDir;

    private static final int indexSize = StoreConfiguration.indexSizeBytes;
    
    /////////////////
    // Fixed header 
    protected static final int    fixedHeaderSize = 2 * NumConversion.BYTES_PER_SHORT;
    // offsets
    protected static final int    formatOffset = 0;
    protected static final int    segmentTypeOffset = formatOffset + NumConversion.BYTES_PER_SHORT;
    // values
    protected static final short  formatVersion = 0;
    protected static final short  segmentType = 0;
    protected static final byte[] fixedHeader;

    ///////////////////
    // Mutable header
    protected static final int    headerSize = fixedHeaderSize + 3 * NumConversion.BYTES_PER_INT;
    // offsets
    protected static final int    segmentNumberOffset = fixedHeaderSize;
    protected static final int    indexOffsetOffset = segmentNumberOffset + NumConversion.BYTES_PER_INT;
    protected static final int    dataOffsetOffset = indexOffsetOffset + NumConversion.BYTES_PER_INT;    
    // values
    /**
     * Currently the indexOffset and dataOffset are fixed.
     */
    protected static final int    indexOffset = headerSize;
    protected static final int    dataOffset = indexOffset + indexSize;    
        
    static {
        fixedHeader = new byte[fixedHeaderSize];
        NumConversion.shortToBytes(formatVersion, fixedHeader, formatOffset);
        NumConversion.shortToBytes(segmentType, fixedHeader, segmentTypeOffset);
    }

    SegmentRWBase(File nsDir) {
        this.nsDir = nsDir;
    }
    
    protected final File fileForSegment(int segmentNumber) {
        return new File(nsDir, Integer.toString(segmentNumber));
    }
}
