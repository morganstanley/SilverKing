package com.ms.silverking.cloud.dht.collection;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

import com.ms.silverking.numeric.NumConversion;

/**
 * PKC with both hash table and full keys stored in ExternalStore.  
 */
public class PartialKeyCuckoo extends PartialKeyIntCuckooBase {
    protected PartialKeyCuckoo(WritableCuckooConfig cuckooConfig, ExternalStore externalStore,
            ByteBuffer hashTableBuffer, boolean initialize) {
        super(cuckooConfig, externalStore, new SubTable[cuckooConfig.getNumSubTables()]);
        for (int i = 0; i < subTables.length; i++) {
            LongBuffer htBuf;

             htBuf = ((ByteBuffer)hashTableBuffer.duplicate().position(subTableBuckets * entriesPerBucket * i *
                     NumConversion.BYTES_PER_LONG)).order(ByteOrder.nativeOrder()).asLongBuffer();
            //htBuf = ((ByteBuffer) hashTableBuffer.duplicate()
            //        .position(subTableBuckets * entriesPerBucket * i * NumConversion.BYTES_PER_LONG)).asLongBuffer();
            subTables[i] = new SubTable(htBuf, subTableBuckets, cuckooConfig.getEntriesPerBucket(), i);
        }
        if (initialize) {
            initialize();
        }
    }
    
    public PartialKeyCuckoo(WritableCuckooConfig cuckooConfig, ExternalStore externalStore,
            ByteBuffer hashTableBuffer) {
        this(cuckooConfig, externalStore, hashTableBuffer, false);
    }
    
    class SubTable extends PKIntSubTableBase {
        private final LongBuffer    htBuf;
        
        SubTable(LongBuffer htBuf, int numBuckets, int entriesPerBucket, int keyShift) {
            super(numBuckets, entriesPerBucket, keyShift);
            this.htBuf = htBuf;
        }
        
        void clear() {
            for (int i = 0; i < bufferSizeLongs; i++) {
                //System.out.printf("%d\t%x\n", i, emptyEntry);
                htBuf.put(i, emptyEntry);
                //setBuf(i, emptyEntry);
            }
        }

        @Override
        protected void setHT(int index, long offset) {
            htBuf.put(index, offset);
        }

        @Override
        protected long getHT(int index) {
            //System.out.printf("%d\t%x\n", index, htBuf.get(index));
            return htBuf.get(index);
        }
    }
}
