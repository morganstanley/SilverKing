package com.ms.silverking.cloud.dht.collection.oldpkc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;

import com.ms.silverking.numeric.NumConversion;


public class MappedValueTableReaderWriter implements ValueTableReaderWriter {
    public MappedValueTableReaderWriter() {
    }

    @Override
    public int getSerializedSizeBytes(ValueTable vt) {
        throw new RuntimeException("invalid for this class");
        /*
        SimpleValueTable    svt;
        
        svt = (SimpleValueTable)vt;
        return NumConversion.BYTES_PER_INT + svt.getSizeBytes();
        */
    }

    @Override
    public void write(ByteBuffer out, ValueTable vt) throws IOException {
        throw new RuntimeException("invalid for this class");
        /*
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
        */
    }

    @Override
    public ValueTable read(ByteBuffer in) throws IOException {
        int length;
        LongBuffer  keys;
        IntBuffer   values;
        
        in = in.asReadOnlyBuffer(); // ensure read-only
        length = in.getInt();
        keys = (LongBuffer)in.asLongBuffer().slice().limit(length * 2);
        in.position(in.position() + NumConversion.BYTES_PER_LONG * length * 2);
        values = (IntBuffer)in.asIntBuffer().slice().limit(length);
        return new MappedFileValueTable(keys, values);
    }
}
