package com.ms.silverking.cloud.dht.collection;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel.MapMode;

import com.ms.silverking.numeric.NumConversion;

/**
 * Reads/writes PartialKeyCuckoo maps (which are RAM-base.)
 */
public class PKCReaderWriter {
    private final ValueTableReaderWriter    vtReaderWriter;
    
    public static final int    overheadBytes = NumConversion.BYTES_PER_INT * 4; 
    
    public PKCReaderWriter(ValueTableReaderWriter vtReaderWriter) {
        this.vtReaderWriter = vtReaderWriter;
    }
    
    /*
    public int getSerializedSizeBytes(PartialKeyCuckoo map) {
        return overheadBytes 
                + map.getPartialIndexSizeBytes() 
                + vtReaderWriter.getSerializedSizeBytes(map.getValueTable());
    }
    
    public void write(File file, PartialKeyCuckoo map) throws IOException {
        RandomAccessFile    raFile;
        
        raFile = new RandomAccessFile(file, "rw");
        try {
            write(raFile.getChannel().map(MapMode.READ_WRITE, 
                0, 
                getSerializedSizeBytes(map)), map);
        } finally {
            raFile.close();
        }
    }
    
    public void write(ByteBuffer out, PartialKeyCuckoo map) throws IOException {
        int numSubTables;
        int entriesPerBucket;
        LongBuffer  longBuffer;
        int subTableSize;
        int totalEntries;
        
        numSubTables = map.getNumSubTables();
        entriesPerBucket = map.getEntriesPerBucket();
        totalEntries = map.getTotalEntries(); 
        out.putInt(totalEntries);
        out.putInt(numSubTables);
        out.putInt(entriesPerBucket);
        subTableSize = map.getSubTable(0).length;
        out.putInt(subTableSize);
        longBuffer = out.asLongBuffer();
        for (int i = 0; i < numSubTables; i++) {
            long[]  subTable;
            
            subTable = map.getSubTable(i);
            if (subTable.length != subTableSize) {
                throw new RuntimeException("subTable.length != subTableSize");
            }
            longBuffer.put(subTable);
        }
        out.position(out.position() + NumConversion.BYTES_PER_LONG * totalEntries);
        vtReaderWriter.write(out, map.getValueTable());
    }
    
    public PartialKeyCuckoo read(File file) throws IOException {
        RandomAccessFile    raFile;
        
        raFile = new RandomAccessFile(file, "r");
        try {
            return read(raFile.getChannel().map(MapMode.READ_ONLY, 
                    0, 
                    file.length()));
        } finally {
            raFile.close();
        }
    }
    
    public PartialKeyCuckoo read(ByteBuffer in) throws IOException {
        int numSubTables;
        int entriesPerBucket;
        int subTableSize;
        LongBuffer  longBuffer;
        long[][] subTables;
        ValueTable  vt;
        int totalEntries;
        
        totalEntries = in.getInt();
        numSubTables = in.getInt();
        entriesPerBucket = in.getInt();
        subTableSize = in.getInt();
        subTables = new long[numSubTables][subTableSize];
        longBuffer = in.asLongBuffer();
        for (int i = 0; i < numSubTables; i++) {
            longBuffer.get(subTables[i]);
        }
        in.position(in.position() + NumConversion.BYTES_PER_LONG * numSubTables * subTableSize);
        vt = vtReaderWriter.read(in);
        return PartialKeyCuckoo.create(totalEntries, numSubTables, entriesPerBucket, vt, subTables);
    }
*/
}
