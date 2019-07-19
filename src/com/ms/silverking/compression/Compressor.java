package com.ms.silverking.compression;

import java.io.IOException;

public interface Compressor {
    public byte[] compress(byte[] rawValue, int offset, int length) throws IOException;
}
