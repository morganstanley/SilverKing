package com.ms.silverking.cloud.dht.collection.oldpkc;

import java.nio.LongBuffer;

import com.ms.silverking.cloud.dht.collection.CuckooConfig;
import com.ms.silverking.cloud.dht.collection.WritableCuckooConfig;
import com.ms.silverking.cloud.dht.collection.oldpkc.PartialKeyCuckooBase.SubTableBase;

public class MappedFilePartialKeyCuckoo extends PartialKeyCuckooBase {
    public MappedFilePartialKeyCuckoo(WritableCuckooConfig cuckooConfig,
            ValueTable valueTable, LongBuffer buf) {
        super(cuckooConfig, valueTable, false, buf);
        for (int i = 0; i < subTables.length; i++) {
            subTables[i] = createSubTable(subTableBuckets, cuckooConfig.getEntriesPerBucket(), i, buf);
        }
    }
    
    public MappedFilePartialKeyCuckoo(CuckooConfig cuckooConfig, ValueTable valueTable, LongBuffer buf) {
        this(WritableCuckooConfig.nonWritableConfig(cuckooConfig), valueTable, buf);
    }
    
    protected SubTableBase createSubTable(int subTableBuckets, int entriesPerBucket, int keyShift, LongBuffer buf) {
        LongBuffer  subTableBuf;
        int         subTableSizeLongs;
        
        subTableSizeLongs = subTableBuckets * entriesPerBucket * SubTable.singleEntrySize;
        subTableBuf = (LongBuffer)buf.slice().limit(subTableSizeLongs);
        buf.position(buf.position() + subTableSizeLongs); // update for the next sub table
        return new SubTable(subTableBuckets, entriesPerBucket, keyShift, subTableBuf);
    }

    class SubTable extends SubTableBase {
        private final LongBuffer    buf;
        private final int       bufferSizeLongs;
        private final int       bucketSizeLongs;
        
        SubTable(int numBuckets, int entriesPerBucket, int keyShift, LongBuffer buf) {
            super(numBuckets, entriesPerBucket, keyShift);
            bucketSizeLongs = singleEntrySize * entriesPerBucket;
            this.bufferSizeLongs = numBuckets * bucketSizeLongs;
            this.buf = buf;
        }

        @Override
        protected int getVTIndex(int baseOffset) {
            return (int)(buf.get(baseOffset) >>> vtIndexShift);
        }

        @Override
        protected void setBuf(int index, long value) {
            buf.put(index, value);
        }

        @Override
        protected long getBuf(int index) {
            return buf.get(index);
        }
    }
}
