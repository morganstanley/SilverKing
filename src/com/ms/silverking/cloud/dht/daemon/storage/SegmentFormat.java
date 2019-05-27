package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.numeric.NumConversion;

/**
 * File format:
 * [header]
 * [data]
 * [index]
 * 
 * Header format:
 * <Field>          <size in bytes>
 * format version   2
 * segment type     2
 * segment number   4
 * index offset     4
 * data offset      4
 */
class SegmentFormat {
    /////////////////
    // Fixed header 
    static final int    fixedHeaderSize = 2 * NumConversion.BYTES_PER_SHORT;
    // offsets
    static final int    formatOffset = 0;
    static final int    segmentTypeOffset = formatOffset + NumConversion.BYTES_PER_SHORT;
    // values
    static final short  formatVersion = 0;
    static final short  segmentType = 0;
    static final byte[] fixedHeader;
    
    ///////////////////
    // Mutable header
    static final int    headerSize = fixedHeaderSize + 3 * NumConversion.BYTES_PER_INT;
    // offsets
    static final int    segmentNumberOffset = fixedHeaderSize;
    static final int    indexOffsetOffset = segmentNumberOffset + NumConversion.BYTES_PER_INT;
    static final int    dataOffsetOffset = indexOffsetOffset + NumConversion.BYTES_PER_INT;
    // values
    /**
     * Currently the indexOffset and dataOffset are fixed.
     */
    //static final int    dataOffset = headerSize;
    
    static {
        fixedHeader = new byte[fixedHeaderSize];
        NumConversion.shortToBytes(formatVersion, fixedHeader, formatOffset);
        NumConversion.shortToBytes(segmentType, fixedHeader, segmentTypeOffset);
    }
    
    static byte[] newHeader(int segmentNumber, int dataOffset, int indexOffset) {
        byte[]  header;
        
        header = new byte[SegmentFormat.headerSize];
        System.arraycopy(SegmentFormat.fixedHeader, 0, header, 0, fixedHeaderSize);
        NumConversion.intToBytes(segmentNumber, header, segmentNumberOffset);
        NumConversion.intToBytes(indexOffset, header, indexOffsetOffset);
        NumConversion.intToBytes(dataOffset, header, dataOffsetOffset);
        return header;
    }
}
