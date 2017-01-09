package com.ms.silverking.cloud.dht.collection.oldpkc;

import com.ms.silverking.cloud.dht.collection.WritableCuckooConfig;
import com.ms.silverking.cloud.dht.collection.oldpkc.PartialKeyCuckooBase.SubTableBase;

public class IntegerArrayPartialKeyCuckoo extends PartialKeyCuckooBase {
    public IntegerArrayPartialKeyCuckoo(WritableCuckooConfig cuckooConfig,
            ValueTable valueTable, boolean initialize) {
        super(cuckooConfig, valueTable, initialize, null);
    }
    
    public IntegerArrayPartialKeyCuckoo(WritableCuckooConfig cuckooConfig) {
        this(cuckooConfig, new SimpleValueTable(cuckooConfig.getTotalEntries()), true);
        for (int i = 0; i < subTables.length; i++) {
            subTables[i] = new SubTable(subTableBuckets, cuckooConfig.getEntriesPerBucket(), i);
        }
    }

    class SubTable extends SubTableBase {
        private final long[]    buf;
        private final int       bufferSizeLongs;
        private final int       bucketSizeLongs;
        
        SubTable(int numBuckets, int entriesPerBucket, int keyShift, long[] buf) {
            super(numBuckets, entriesPerBucket, keyShift);
            bucketSizeLongs = singleEntrySize * entriesPerBucket;
            this.bufferSizeLongs = numBuckets * bucketSizeLongs;
            if (buf == null) {
                this.buf = new long[bufferSizeLongs];
            } else {
                this.buf = buf;
            }
        }

        SubTable(int numBuckets, int entriesPerBucket, int keyShift) {
            this(numBuckets, entriesPerBucket, keyShift, new long[numBuckets * entriesPerBucket]);
        }
        
        @Override
        protected int getVTIndex(int baseOffset) {
            return (int)(buf[baseOffset] >>> vtIndexShift);
        }

        @Override
        protected void setBuf(int index, long value) {
            buf[index] = value;
        }

        @Override
        protected long getBuf(int index) {
            return buf[index];
        }
    }
}
