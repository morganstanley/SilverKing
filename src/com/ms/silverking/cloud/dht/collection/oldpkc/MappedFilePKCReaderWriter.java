package com.ms.silverking.cloud.dht.collection.oldpkc;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel.MapMode;

import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.cloud.dht.collection.CuckooConfig;

/**
 * Reads MappedFilePartialKeyCuckoo maps.
 */
public class MappedFilePKCReaderWriter {
    private final ValueTableReaderWriter    vtReaderWriter;
    
    public MappedFilePKCReaderWriter(ValueTableReaderWriter vtReaderWriter) {
        this.vtReaderWriter = vtReaderWriter;
    }
    
    public MappedFilePartialKeyCuckoo read(File file) throws IOException {
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
    
    public MappedFilePartialKeyCuckoo read(ByteBuffer in) throws IOException {
        int numSubTables;
        int entriesPerBucket;
        int subTableSize;
        LongBuffer  longBuffer;
        ValueTable  vt;
        int totalEntries;
        
        in = in.asReadOnlyBuffer(); // ensure read-only
        totalEntries = in.getInt();
        numSubTables = in.getInt();
        entriesPerBucket = in.getInt();
        subTableSize = in.getInt();
        
        longBuffer = in.asLongBuffer();
        in.position(in.position() + NumConversion.BYTES_PER_LONG * totalEntries);
        vt = vtReaderWriter.read(in);
        return new MappedFilePartialKeyCuckoo(new CuckooConfig(totalEntries, numSubTables, entriesPerBucket), vt, longBuffer);
    }
}
