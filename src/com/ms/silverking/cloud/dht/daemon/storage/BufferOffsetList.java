package com.ms.silverking.cloud.dht.daemon.storage;

import java.nio.ByteBuffer;

import com.ms.silverking.log.Log;
import com.ms.silverking.numeric.NumConversion;

/**
 * Read-only OffsetList stored on disk.
 */
public final class BufferOffsetList extends OffsetListBase {
    private final ByteBuffer    buf;    
    
    private final boolean   debug = false;
    
    BufferOffsetList(ByteBuffer buf, boolean supportsStorageTime) {
        super(supportsStorageTime);
        this.buf = buf;
        //this.buf = buf.order(ByteOrder.BIG_ENDIAN);
        if (debug) {
            System.out.println(buf.order());
            System.out.println(buf);
            for (int i = 0; i < buf.limit(); i += 4) {
                System.out.printf("%d\t%d\t%x\n", i, buf.getInt(i), buf.getInt(i));
                //System.out.printf("%d\t%d\t%x\n", i, buf.get(i), buf.get(i));
            }
        }
    }
    
    public int getIndex() {
        return buf.getInt(indexOffset);
    }
    
    protected int entryBaseOffsetBytes(int index) {
        return super.entryBaseOffset(index) * NumConversion.BYTES_PER_INT + persistedHeaderSizeBytes;
    }
    
    @Override
    public int size() {
        return buf.getInt(listSizeOffset);
        //return buf.getInt(listSizeOffset) / entrySizeInts;
    }
    
    @Override
    protected long getVersion(int index) {
        if (debug) {
            Log.warning("index "+ index);
            Log.warning("entryBaseOffsetBytes(index) "+ entryBaseOffsetBytes(index));
        }
        return buf.getLong(entryBaseOffsetBytes(index) + versionOffset);
    }
    
    @Override
    protected long getStorageTime(int index) {
        if (debug) {
            Log.warning("index "+ index);
            Log.warning("entryBaseOffsetBytes(index) "+ entryBaseOffsetBytes(index));
        }
        return buf.getLong(entryBaseOffsetBytes(index) + storageTimeOffset);
    }
    
    protected int getOffset(int index) {
        if (debug) {
            Log.warning("BufferOffsetList.getOffset: "+ index +" "+ buf.getInt(entryBaseOffsetBytes(index) + offsetOffset * NumConversion.BYTES_PER_INT));
        }
        try {
        	return buf.getInt(entryBaseOffsetBytes(index) + offsetOffset * NumConversion.BYTES_PER_INT);
        } catch (IndexOutOfBoundsException e) {
            Log.warningf("%d %d %d", index, entryBaseOffsetBytes(index), entryBaseOffsetBytes(index) + offsetOffset * NumConversion.BYTES_PER_INT);
            Log.warningf("%s", buf);
            throw e;
        }
    }
        
    @Override
    public void putOffset(long version, int offset, long storageTime) {
        throw new UnsupportedOperationException();
    }
}
