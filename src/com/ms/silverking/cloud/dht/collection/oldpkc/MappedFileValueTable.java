package com.ms.silverking.cloud.dht.collection.oldpkc;

import java.nio.IntBuffer;
import java.nio.LongBuffer;

import com.ms.silverking.numeric.NumConversion;


public final class MappedFileValueTable implements ValueTable {
    private final int           numValues;
    private final LongBuffer    keys;
    private final IntBuffer     values;
    private int nextEntry;
    
    private static final int    mslOffset = 0;
    private static final int    lslOffset = 1;
    private static final int    entrySize = 2;
    
    public MappedFileValueTable(LongBuffer keys, IntBuffer values) {
        this.keys = keys;
        this.values = values;
        numValues = values.capacity();
    }
    
    @Override
    public int getSizeBytes() {
        return numValues * 2 * NumConversion.BYTES_PER_LONG * 2 
             + numValues * NumConversion.BYTES_PER_INT;
    }
    
    public void clear() {
        for (int i = 0; i < numValues; i++) {
            //values[i] = 0;
            values.put(i, 0);
            //keys[i * 2] = 0;
            keys.put(i * 2, 0);
            //keys[i * 2 + 1] = 0;
            keys.put(i * 2 + 1, 0);
        }
        nextEntry = 0;
    }
    
    @Override
    public int add(long msl, long lsl, int value) {
        int index;
        
        index = nextEntry++;
        store(index, msl, lsl, value);
        return index;
    }
    
    @Override
    public void store(int index, long msl, long lsl, int value) {
        int baseOffset;
        
        baseOffset = index * entrySize;
        keys.put(baseOffset + mslOffset, msl);
        keys.put(baseOffset + lslOffset, lsl);
        values.put(index, value);
        //System.out.printf("store values[%d]\t%d\n", index, values[index]);
    }

    @Override
    public int matches(int index, long msl, long lsl) {
        int baseOffset;
        
        baseOffset = index * entrySize;
        if (keys.get(baseOffset + mslOffset) == msl
            && keys.get(baseOffset + lslOffset) == lsl) {
          return values.get(index);  
        } else {
            return noMatch;
        }
    }

    @Override
    public long getMSL(int index) {
        int baseOffset;
        
        baseOffset = index * entrySize;
        return keys.get(baseOffset + mslOffset);
    }

    @Override
    public long getLSL(int index) {
        int baseOffset;
        
        baseOffset = index * entrySize;
        return keys.get(baseOffset + lslOffset);
    }
    
    @Override
    public int getValue(int index) {
        //System.out.printf("getValue values[%d]\t%d\n", index, values[index]);
        return values.get(index);
    }
    /*
    long[] getKeys() {
        return keys;
    }
    
    int[] getValues() {
        return values;
    }
    */
}
