package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.compression.BZip2;
import com.ms.silverking.compression.Compressor;
import com.ms.silverking.compression.Decompressor;
import com.ms.silverking.compression.LZ4;
import com.ms.silverking.compression.Snappy;
import com.ms.silverking.compression.Zip;

public class CodecProvider {
    public static Compressor getCompressor(Compression compression) {
        switch (compression) {
        case LZ4: return new LZ4();
        case SNAPPY: return new Snappy();
        case ZIP: return new Zip();
        case BZIP2: return new BZip2();
        case NONE: return null;
        default: throw new RuntimeException("No compressor for "+ compression);
        }
    }
    
    public static Decompressor getDecompressor(Compression compression) {
        switch (compression) {
        case LZ4: return new LZ4();
        case SNAPPY: return new Snappy();
        case ZIP: return new Zip();
        case BZIP2: return new BZip2();
        case NONE: return null;
        default: throw new RuntimeException("No decompressor for "+ compression);
        }
    }    
}
