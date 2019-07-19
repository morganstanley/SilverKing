package com.ms.silverking.cloud.dht.collection;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.ms.silverking.numeric.NumConversion;


public class SVTReaderWriter implements ValueTableReaderWriter {
    public static final int    overheadBytes = NumConversion.BYTES_PER_INT;
    
    public SVTReaderWriter() {
    }

    @Override
    public int getSerializedSizeBytes(ValueTable vt) {
        SimpleValueTable    svt;
        
        svt = (SimpleValueTable)vt;
        return overheadBytes + svt.getSizeBytes();
    }

    @Override
    public void write(ByteBuffer out, ValueTable vt) throws IOException {
        SimpleValueTable    svt;
        
        svt = (SimpleValueTable)vt;
        assert svt.getKeys().length == svt.getValues().length;
        out.putInt(svt.getValues().length);
        for (long key : svt.getKeys()) {
            out.putLong(key);
        }
        for (int value : svt.getValues()) {
            out.putInt(value);
        }
    }

    @Override
    public ValueTable read(ByteBuffer in) throws IOException {
        int length;
        long[]  keys;
        int[]   values;
        
        length = in.getInt();
        keys = new long[length * 2];
        values = new int[length];
        for (int i = 0; i < length * 2; i++) {
            keys[i] = in.getLong();
        }
        for (int i = 0; i < length; i++) {
            values[i] = in.getInt();
        }
        return new SimpleValueTable(keys, values);
    }
}
