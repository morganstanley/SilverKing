package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import com.ms.silverking.cloud.dht.collection.PKCReaderWriter;
import com.ms.silverking.cloud.dht.collection.PartialKeyCuckoo;
import com.ms.silverking.cloud.dht.collection.SVTReaderWriter;
import com.ms.silverking.numeric.NumConversion;

// FUTURE consider deprecating this class

/**
 * Prototype class to persist RAMSegments on disk where they can be used as FileSegments.
 * Current approach is to include the index with every segment while holding open the 
 * possibility of an additional index.
 * 
 */
class SegmentWriter extends SegmentRWBase {
    private final PKCReaderWriter         pkcReaderWriter;
    
    SegmentWriter(File nsDir) {
        super(nsDir);
        pkcReaderWriter = new PKCReaderWriter(new SVTReaderWriter());
    }
    
    private long persistedSegmentSize(RAMSegment segment) {
        return segment.totalSize() + headerSize + PKCReaderWriter.overheadBytes + SVTReaderWriter.overheadBytes;
    }
        
    void writeToFile(RAMSegment segment, int segmentNumber) throws IOException {
        File    segmentFile;
        byte[]  header;
        ByteBuffer  segmentBuf;
        RandomAccessFile    raFile;
        
        segmentFile = fileForSegment(segmentNumber);
        header = new byte[headerSize];
        System.arraycopy(fixedHeader, 0, header, 0, fixedHeaderSize);
        NumConversion.intToBytes(segmentNumber, header, segmentNumberOffset);
        NumConversion.intToBytes(indexOffset, header, indexOffsetOffset);
        NumConversion.intToBytes(dataOffset, header, dataOffsetOffset);
        
        raFile = new RandomAccessFile(segmentFile, "rw");
        try {
            segmentBuf = raFile.getChannel().map(MapMode.READ_WRITE, 0, persistedSegmentSize(segment));
            segmentBuf.put(header);
            writeIndex(segmentBuf, segment);
            //segmentBuf.put(segment.getData());
            /*
            FileUtil.writeToFile(segmentFile, header);
            writeIndex(segmentFile, segment);
            FileUtil.writeToFile(segmentFile, segment.getData());
            */
        } finally {
            raFile.close();
        }
    }
    
    private void writeIndex(ByteBuffer buf, RAMSegment segment) throws IOException {
        PartialKeyCuckoo    index;
        
        //index = segment.getIndex();
        //pkcReaderWriter.write(buf, index);
    }
}
