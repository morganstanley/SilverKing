package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel.MapMode;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.collection.CuckooConfig;
import com.ms.silverking.cloud.dht.collection.HybridPartialKeyCuckoo;
import com.ms.silverking.cloud.dht.collection.WritableCuckooConfig;
import com.ms.silverking.log.Log;
import com.ms.silverking.numeric.NumConversion;

public class FileSegment extends WritableSegmentBase {
    private int                 references;
    private RandomAccessFile    raFile;
    
    private static final String    roFileMode = "r";
    private static final String    rwFileMode = "rw";
    private static final String    rwdFileMode = "rwd";
    
    private final int   noReferences = Integer.MIN_VALUE;
    
    enum SyncMode {NoSync, Sync};
    
    enum BufferMode {PreRead, InPlace};
    
    private static String syncModeToFileOpenMode(SyncMode syncMode) {
        return syncMode == SyncMode.NoSync ? rwFileMode : rwdFileMode;
    }
    
    static final FileSegment create(File nsDir, int segmentNumber, int dataSegmentSize, SyncMode syncMode, 
                                    NamespaceOptions nsOptions) throws IOException {
        RandomAccessFile    raFile;
        byte[]              header;
        ByteBuffer          dataBuf;
        int                 indexOffset;
        
        indexOffset = dataSegmentSize;
        raFile = new RandomAccessFile(fileForSegment(nsDir, segmentNumber), syncModeToFileOpenMode(syncMode));
        header = SegmentFormat.newHeader(segmentNumber, dataOffset, indexOffset);
        
        dataBuf = raFile.getChannel().map(MapMode.READ_WRITE, 0, dataSegmentSize);
        dataBuf.put(header);
        //raFile.getFD().sync(); // For now we leave this out and let SyncMode cover this
        return new FileSegment(nsDir, segmentNumber, raFile, dataBuf, dataSegmentSize, nsOptions);
    }
    
    public static FileSegment openForDataUpdate(File nsDir, int segmentNumber, int dataSegmentSize,
                                                SyncMode syncMode, NamespaceOptions nsOptions) throws IOException {
        return open(nsDir, segmentNumber, MapMode.READ_WRITE, dataSegmentSize, syncMode, nsOptions, BufferMode.InPlace);
    }
    
    public static FileSegment openReadOnly(File nsDir, int segmentNumber, int dataSegmentSize, 
            NamespaceOptions nsOptions) throws IOException {
        return openReadOnly(nsDir, segmentNumber, dataSegmentSize, nsOptions, BufferMode.InPlace);
    }
    
    public static FileSegment openReadOnly(File nsDir, int segmentNumber, int dataSegmentSize, 
                                           NamespaceOptions nsOptions, BufferMode bufferMode) throws IOException {
        return open(nsDir, segmentNumber, MapMode.READ_ONLY, dataSegmentSize, SyncMode.NoSync, nsOptions, bufferMode);
    }
    
    private static FileSegment open(File nsDir, int segmentNumber, MapMode dataMapMode, int dataSegmentSize, 
                                    SyncMode syncMode, NamespaceOptions nsOptions, BufferMode bufferMode) 
                                            throws IOException {
        RandomAccessFile    raFile;
        ByteBuffer          dataBuf;
        ByteBuffer          rawHTBuf;
        ByteBuffer          htBuf;
        int                 htBufSize;
        int                 htTotalEntries;
        String              fileOpenMode;
        WritableCuckooConfig        segmentCuckooConfig;

        if (dataMapMode == MapMode.READ_ONLY) {
            fileOpenMode = roFileMode;
        } else if (dataMapMode == MapMode.READ_WRITE) {
            fileOpenMode = syncModeToFileOpenMode(syncMode);
        } else if (dataMapMode == MapMode.PRIVATE) {
            throw new RuntimeException("MapMode.PRIVATE currently unsupported");
        } else {
            throw new RuntimeException("Unexpected dataMapMode: "+ dataMapMode);
        }
        raFile = new RandomAccessFile(fileForSegment(nsDir, segmentNumber), fileOpenMode);
        if (bufferMode == BufferMode.PreRead) {
            byte[]      _bufArray;
            
            _bufArray = new byte[dataSegmentSize];
            raFile.seek(0);
            raFile.read(_bufArray);
            dataBuf = ByteBuffer.wrap(_bufArray);
        } else {
            dataBuf = raFile.getChannel().map(dataMapMode, 0, dataSegmentSize);
        }
        rawHTBuf = raFile.getChannel().map(MapMode.READ_ONLY, dataSegmentSize, raFile.length() - dataSegmentSize);
        rawHTBuf = rawHTBuf.order(ByteOrder.nativeOrder());
        try {
        htBuf = ((ByteBuffer)rawHTBuf.duplicate().position(NumConversion.BYTES_PER_INT + CuckooConfig.BYTES)).slice();
        } catch (RuntimeException re) {
            System.out.println(nsDir);
            System.out.println(segmentNumber);
            System.out.println(dataMapMode);
            System.out.println();
            System.out.println(dataBuf);
            System.out.printf("%d\t%d\t%d\t%d\n", dataSegmentSize, raFile.length(), 
                              dataSegmentSize, raFile.length() - dataSegmentSize);
            System.out.println();
            System.out.println(rawHTBuf);
            throw re;
        }
        htBuf = htBuf.order(ByteOrder.nativeOrder());
        // FUTURE - cache the below number or does the segment cache do this well enough? (also cache the segmentCuckooConfig...)
        htBufSize = rawHTBuf.getInt(0);
        segmentCuckooConfig = new WritableCuckooConfig(CuckooConfig.read(rawHTBuf, NumConversion.BYTES_PER_INT), -1); // FIXME - verify -1
        
        htTotalEntries = htBufSize / NumConversion.BYTES_PER_LONG;
        return new FileSegment(nsDir, segmentNumber, raFile, dataBuf, htBuf, 
                               segmentCuckooConfig.newTotalEntries(htTotalEntries), 
                               new BufferOffsetListStore(rawHTBuf, nsOptions), dataSegmentSize);
    }
    
    static File fileForSegment(File nsDir, int segmentNumber) {
        return new File(nsDir, Integer.toString(segmentNumber));
    }
    
    public static ByteBuffer getDataSegment(File nsDir, int segmentNumber, int dataSegmentSize) throws IOException {
        RandomAccessFile    raFile;
        ByteBuffer          dataBuf;
        
        raFile = new RandomAccessFile(fileForSegment(nsDir, segmentNumber), roFileMode);
        dataBuf = raFile.getChannel().map(MapMode.READ_ONLY, 0, dataSegmentSize);
        raFile.close();
        return dataBuf;
    }
    
    static final FileSegment openForRecovery(File nsDir, int segmentNumber, int dataSegmentSize, 
                                            SyncMode syncMode, NamespaceOptions nsOptions) throws IOException {
        RandomAccessFile    raFile;
        ByteBuffer          dataBuf;
        FileSegment         segment;
        
        raFile = new RandomAccessFile(fileForSegment(nsDir, segmentNumber), syncModeToFileOpenMode(syncMode));
        
        dataBuf = raFile.getChannel().map(MapMode.READ_WRITE, 0, dataSegmentSize);
        segment = new FileSegment(nsDir, segmentNumber, raFile, dataBuf, dataSegmentSize, nsOptions);
        return segment;
    }
    
    /*
    private FileSegment(File nsDir, int segmentNumber, RandomAccessFile raFile, ByteBuffer dataBuf,
                        PartialKeyCuckooBase pkc) throws IOException {
        super(dataBuf);
        this.segmentNumber = segmentNumber;
        this.raFile = raFile;
        this.pkc = pkc;
        nextFree = new AtomicInteger(SegmentFormat.headerSize);
    }
    */
    
    // called from openReadOnly
    private FileSegment(File nsDir, int segmentNumber, RandomAccessFile raFile, ByteBuffer dataBuf, ByteBuffer htBuf,
            WritableCuckooConfig cuckooConfig, BufferOffsetListStore bufferOffsetListStore, int dataSegmentSize) 
                    throws IOException {
        super(nsDir, segmentNumber, dataBuf, htBuf, cuckooConfig, bufferOffsetListStore, dataSegmentSize);
        this.raFile = raFile;
    }

    // called from open for recovery
    /*
    private FileSegment(File nsDir, int segmentNumber, RandomAccessFile raFile, ByteBuffer dataBuf,
                        PartialKeyIntCuckooBase pkc) throws IOException {
        super(nsDir, segmentNumber, dataBuf, pkc);
        this.raFile = raFile;
    }
    */
    
    // called from Create
    private FileSegment(File nsDir, int segmentNumber, RandomAccessFile raFile, ByteBuffer dataBuf, 
            int dataSegmentSize, NamespaceOptions nsOptions) 
                    throws IOException {
        super(nsDir, segmentNumber, dataBuf, StoreConfiguration.fileInitialCuckooConfig, dataSegmentSize, nsOptions);
        this.raFile = raFile;
    }
    
    public void persist() throws IOException {
        ByteBuffer  htBuf;
        long[]      ht;
        int         offsetStoreSize;
        int         htBufSize;
        long        mapSize;
        int         htPersistedSize;
        
        offsetStoreSize = ((RAMOffsetListStore)offsetListStore).persistedSizeBytes();
        
        if (debugPut) {
            System.out.printf("offsetStoreSize %d\n", offsetStoreSize);
            System.out.printf("raFile.length() %d\n", raFile.length());
        }
        
        ht = ((HybridPartialKeyCuckoo)pkc).getHashTableArray();
        htBufSize = ht.length * NumConversion.BYTES_PER_LONG;
        htPersistedSize = NumConversion.BYTES_PER_INT + htBufSize + CuckooConfig.BYTES;
        mapSize = htPersistedSize + offsetStoreSize;
        htBuf = raFile.getChannel().map(MapMode.READ_WRITE, dataSegmentSize, mapSize).order(ByteOrder.nativeOrder());
        if (debugPut) {
            System.out.printf("b raFile.length() %d %s\n", raFile.length(), raFile.toString());
        }

        // Store the size of the ht, then the config
        htBuf.putInt(htBufSize);
        pkc.getConfig().persist(htBuf, NumConversion.BYTES_PER_INT);
        if (debugPut) {
            System.out.printf("\tpersist htBufSize: %d\tmapSize: %d\n", htBufSize, mapSize);
            System.out.printf("c raFile.length() %d %s\n", raFile.length(), raFile.toString());
        }
        htBuf.position(NumConversion.BYTES_PER_INT + CuckooConfig.BYTES);
        
        // Persist the ht itself
        htBuf.asLongBuffer().put(ht);
        htBuf.position(htPersistedSize);
        
        // Now persist the offsetListStore
        ((RAMOffsetListStore)offsetListStore).persist(htBuf);
        //((sun.nio.ch.DirectBuffer)htBuf).cleaner().clean();
        
        raFile.getChannel().force(true);
        raFile.getFD().sync();
        close();
    }
    
    /*
     * We implement simple reference counting for the read-only case since we want to shut the file
     * as soon as possible. Waiting for finalization is too long. 
     */
    
    public boolean addReference() {
        return addReferences(1);
    }
    
    public boolean addReferences(int numReferences) {
        assert numReferences > 0;
        synchronized (this) {
            if (references != noReferences) {
                references += numReferences;
                return true;
            } else {
                return false;
            }
        }
    }
    
    public void removeReference() {
        synchronized (this) {
            if (references <= 0) {
                throw new RuntimeException("Invalid removeReference: "+ this +" "+ references);
            }
            --references;
            if (references == 0) {
                references = noReferences;
                close();
            }
        }
    }
    
    @Override
    public void finalize() {
        close();
    }
    
    public void close() {
        // FUTURE - can we close raFile earlier?        
        try {
            raFile.close();
            raFile = null;
            dataBuf = null;
            //System.gc();
            //System.runFinalization();
        } catch (IOException ioe) {
            Log.logErrorWarning(ioe);
        }
    }

    public void displayForDebug() {
        int i;
        
        i = 1;
        while (true) {
            OffsetList  offsetList;
            
            try {
                offsetList = offsetListStore.getOffsetList(i);
            } catch (RuntimeException e) {
                offsetList = null;
            }
            if (offsetList != null) {
                System.out.println("\nOffset list: "+ i);
                offsetList.displayForDebug();
                i++;
            } else {
                break;
            }
        }
    }
}
