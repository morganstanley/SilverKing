package com.ms.silverking.compression;

import com.ms.silverking.cloud.dht.client.Compression;

public class CodecProvider {
	private static final LZ4	lz4 = new LZ4();
	private static final Snappy	snappy = new Snappy();
	private static final Zip	zip = new Zip();
	private static final BZip2	bzip2 = new BZip2();
	static final NullCodec	nullCodec = new NullCodec();
	
	static {
		
	}
	
    public static Compressor getCompressor(Compression compression) {
        switch (compression) {
        case LZ4: return lz4;
        case SNAPPY: return snappy;
        case ZIP: return zip;
        case BZIP2: return bzip2;
        case NONE: return null;
        default: throw new RuntimeException("No compressor for "+ compression);
        }
    }
    
    public static Decompressor getDecompressor(Compression compression) {
        switch (compression) {
        case LZ4: return lz4;
        case SNAPPY: return snappy;
        case ZIP: return zip;
        case BZIP2: return bzip2;
        case NONE: return null;
        default: throw new RuntimeException("No decompressor for "+ compression);
        }
    }
}
