package com.ms.silverking.cloud.dht.crypto;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.ms.silverking.cloud.dht.client.impl.Checksum;
import com.ms.silverking.io.util.BufferUtil;

public class HashFunctionChecksum implements Checksum {
    private final HashFunction   hashFunction;
    private final int            bytes;    
    private final byte[]         emptyChecksum;
    
    public enum Type {Murmur3_32, Murmur3_128, /*Adler32*/};
    
    public HashFunctionChecksum(Type type) {
        switch (type) {
        case Murmur3_32:
            hashFunction = Hashing.murmur3_32();
            break;
        case Murmur3_128:
            hashFunction = Hashing.murmur3_128();
            break;
            /*
        case Adler32:
            hashFunction = Hashing.adler32();
            break;
            */
        default: throw new RuntimeException("panic");
        }
        bytes = hashFunction.bits() / 8;
        emptyChecksum = new byte[bytes];
    }
    
    @Override
    public void checksum(byte[] source, int position, int length, ByteBuffer dest) {
        dest.put(checksum(source, position, length));
    }
    
    @Override
    public byte[] checksum(byte[] source, int position, int length) {
        HashCode    hashCode;
        
        hashCode = hashFunction.hashBytes(source, position, length);
        return hashCode.asBytes();
    }

    @Override
    public void checksum(ByteBuffer source, ByteBuffer dest) {
        dest.put(checksum(source));
    }

    @Override
    public byte[] checksum(ByteBuffer source) {
        HashCode    hashCode;
        
        if (source.hasArray()) {
            hashCode = hashFunction.hashBytes(source.array(), source.position(), source.remaining());
        } else {
            byte[]  tmp;
            
            tmp = new byte[source.remaining()];
            BufferUtil.get(source, source.position(), tmp, tmp.length);
            hashCode = hashFunction.hashBytes(tmp);
        }
        return hashCode.asBytes();
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
}
