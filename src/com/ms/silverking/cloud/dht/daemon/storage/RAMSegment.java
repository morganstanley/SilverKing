package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel.MapMode;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.collection.IntArrayCuckoo;
import com.ms.silverking.numeric.NumConversion;

class RAMSegment extends WritableSegmentBase {
    static final RAMSegment create(File nsDir, int segmentNumber, int dataSegmentSize, NamespaceOptions nsOptions) {
        RandomAccessFile    raFile;
        byte[]              header;
        ByteBuffer          dataBuf;
        int                 indexOffset;
        
        indexOffset = dataSegmentSize;
        header = SegmentFormat.newHeader(segmentNumber, dataOffset, indexOffset);
        
        //dataBuf = ByteBuffer.allocate(dataSegmentSize);
        dataBuf = ByteBuffer.allocateDirect(dataSegmentSize);
        dataBuf.put(header);
        return new RAMSegment(nsDir, segmentNumber, dataBuf, dataSegmentSize, nsOptions);
    }
    
    private static File fileForSegment(File nsDir, int segmentNumber) {
        return new File(nsDir, Integer.toString(segmentNumber));
    }
    
    // called from Create
    //private RAMSegment(File nsDir, int segmentNumber, RandomAccessFile raFile, ByteBuffer dataBuf) throws IOException {
    private RAMSegment(File nsDir, int segmentNumber, ByteBuffer dataBuf, int dataSegmentSize, 
                       NamespaceOptions nsOptions) {
        super(nsDir, segmentNumber, dataBuf, StoreConfiguration.ramInitialCuckooConfig, dataSegmentSize, nsOptions);
    }

    @Override
    public void persist() throws IOException {
        ByteBuffer  htBuf;
        byte[]      ht;
        int         offsetStoreSize;
        int         htBufSize;
        long        mapSize;
        
        offsetStoreSize = ((RAMOffsetListStore)offsetListStore).persistedSizeBytes();
        
        RandomAccessFile    raFile;
        
        raFile = new RandomAccessFile(fileForSegment(nsDir, segmentNumber), "rw");
        try {        
	        raFile.write(dataBuf.array());
	        
	        ht = ((IntArrayCuckoo)keyToOffset).getAsBytes();
	        htBufSize = ht.length;
	        mapSize = NumConversion.BYTES_PER_INT + htBufSize + offsetStoreSize;
	        htBuf = raFile.getChannel().map(MapMode.READ_WRITE, dataSegmentSize, mapSize).order(ByteOrder.nativeOrder());
	        htBuf.putInt(htBufSize);
	        //System.out.printf("\tpersist htBufSize: %d\tmapSize: %d\n", htBufSize, mapSize);
	        htBuf.put(ht);
	        htBuf.position(NumConversion.BYTES_PER_INT + htBufSize);
	        ((RAMOffsetListStore)offsetListStore).persist(htBuf);
	        //((sun.nio.ch.DirectBuffer)htBuf).cleaner().clean();
        } finally {
        	raFile.close();
        }
        close();
    }
    
    public void close() {
    }
    
	public SegmentCompactionResult compact() {
		throw new RuntimeException("Compaction not supported for RAMSegment");
	}    
}
