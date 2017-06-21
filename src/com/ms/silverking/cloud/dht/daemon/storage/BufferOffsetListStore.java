package com.ms.silverking.cloud.dht.daemon.storage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.collection.CuckooConfig;
import com.ms.silverking.log.Log;
import com.ms.silverking.numeric.NumConversion;

public class BufferOffsetListStore implements OffsetListStore {
    private final ByteBuffer    rawHTBuf;
    private ByteBuffer    buf;
    private final NamespaceOptions  nsOptions;
    
    /*
     * As discussed in RAMOffsetListStore, indexing to 
     * the lists is 1-based externally and zero-based internally. 
     */
    
    BufferOffsetListStore(ByteBuffer rawHTBuf, NamespaceOptions nsOptions) {
        this.rawHTBuf = rawHTBuf;
        this.nsOptions = nsOptions;
        ensureBufInitialized(); // eager
    }
    
    private void ensureBufInitialized() {
        if (buf == null) {
            int htBufSize;
            
            htBufSize = rawHTBuf.getInt(0);
            if (debug) {
                System.out.println("\thtBufSize: "+ htBufSize);
            }
            buf = ((ByteBuffer)rawHTBuf.duplicate().position(htBufSize + CuckooConfig.BYTES + NumConversion.BYTES_PER_INT)).slice().order(ByteOrder.nativeOrder());
        }
    }

    @Override
    public OffsetList newOffsetList() {
        throw new UnsupportedOperationException();
    }

    @Override
    public OffsetList getOffsetList(int index) {
    	try {
	        ByteBuffer  listBuf;
	        int         listOffset;
	        int         listNumEntries;
	        int         listSizeBytes;
	        boolean     supportsStorageTime;
	        
	        //ensureBufInitialized(); // lazy, chance of multiple calls
	        if (index < 0) {
	            throw new RuntimeException("panic: "+ index);
	        }
	        if (debug) {
	            System.out.println("getOffsetList index: "+ index);
	        }
	        supportsStorageTime = nsOptions.getRevisionMode() == RevisionMode.UNRESTRICTED_REVISIONS;
	        // in the line below, the -1 to translate to an internal index
	        // and the +1 of the length offset cancel out
	        listOffset = buf.getInt(index * NumConversion.BYTES_PER_INT);
	        if (listOffset < 0 || listOffset >= buf.limit()) {
	        	throw new InvalidOffsetListIndexException(index);
	        }
	        if (debug) {
	            System.out.printf("%d\t%x\n", listOffset, listOffset);
	            System.out.println(buf);
	        }
	        listBuf = ((ByteBuffer)buf.duplicate().position(listOffset)).slice().order(ByteOrder.nativeOrder());
	        listNumEntries = buf.getInt(listOffset + OffsetListBase.listSizeOffset);
	        listSizeBytes = listNumEntries * OffsetListBase.entrySizeBytes(supportsStorageTime) + OffsetListBase.persistedHeaderSizeBytes;
	        if (debug) {
	            System.out.printf("index %d\tlistNumEntries %d\tlistSizeBytes %d\n", index, listNumEntries, listSizeBytes);
	        }
	        listBuf.limit(listSizeBytes);
	        return new BufferOffsetList(listBuf, supportsStorageTime);
    	} catch (RuntimeException re) { // FUTURE - consider removing debug
	        int         listOffset;
	        
	        listOffset = buf.getInt(index * NumConversion.BYTES_PER_INT);
            Log.warningf("getOffsetList index: %d", index);
            Log.warningf("%d\t%x\n", listOffset, listOffset);
            Log.warningf("%s", buf.duplicate());
    		re.printStackTrace();
    		throw re;
    	}
    }
}
