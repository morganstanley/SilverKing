package com.ms.silverking.compression;

import java.io.IOException;

public interface Decompressor {
    public byte[] decompress(byte[] value, int offset, int length, 
                             int uncompressedLength) throws IOException;
}
