package com.ms.silverking.compression;

import java.io.IOException;

public class NullCodec implements Compressor, Decompressor {
    public NullCodec() {
    }
    
    public byte[] compress(byte[] rawValue, int offset, int length) throws IOException {
    	byte[]	b;
    	
    	b = new byte[length];
    	System.arraycopy(rawValue, offset, b, 0, length);
    	return b;
    }

    public byte[] decompress(byte[] value, int offset, int length, int uncompressedLength) throws IOException {
    	byte[]	b;
    	
    	if (length != uncompressedLength) {
    		throw new RuntimeException("length != uncompressedLength");
    	}
    	b = new byte[length];
    	System.arraycopy(value, offset, b, 0, length);
    	return b;
    }
}
