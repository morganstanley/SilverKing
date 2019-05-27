package com.ms.silverking.cloud.dht.client.impl;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.ms.silverking.numeric.NumConversion;

public class SystemChecksum implements Checksum {
	public static final int	BYTES = 8;
	
    private static final byte[] emptyChecksum = new byte[BYTES];
    private static final byte[] invalidationChecksum;

    static {
    	invalidationChecksum = NumConversion.longToBytes(0xdeadbeef);
    }
    
	public SystemChecksum() {
	}

    
    @Override
    public void checksum(byte[] source, int position, int length, ByteBuffer dest) {
        dest.put(checksum(source, position, length));
    }
    
    @Override
    public byte[] checksum(byte[] source, int position, int length) {
    	return invalidationChecksum;
    }

    @Override
    public void checksum(ByteBuffer source, ByteBuffer dest) {
        dest.put(checksum(source));
    }

    @Override
    public byte[] checksum(ByteBuffer source) {
        return invalidationChecksum;
    }

    @Override
    public void emptyChecksum(ByteBuffer dest) {
        dest.put(emptyChecksum);
    }

    @Override
    public boolean isEmpty(byte[] checksum) {
        return Arrays.equals(checksum, emptyChecksum);
    }

    @Override
    public boolean uniquelyIdentifiesValues() {
        return false;
    }

	public static boolean isInvalidationChecksum(byte[] actualChecksum) {
		return Arrays.equals(actualChecksum, invalidationChecksum);
	}
}
