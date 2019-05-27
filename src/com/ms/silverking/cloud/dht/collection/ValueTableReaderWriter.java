package com.ms.silverking.cloud.dht.collection;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface ValueTableReaderWriter {
    public int getSerializedSizeBytes(ValueTable vt);
    public void write(ByteBuffer out, ValueTable vt) throws IOException;
    public ValueTable read(ByteBuffer in) throws IOException;
}
