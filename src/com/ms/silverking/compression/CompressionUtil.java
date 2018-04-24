package com.ms.silverking.compression;

import java.io.IOException;

import com.ms.silverking.cloud.dht.client.Compression;

public class CompressionUtil {
	public static byte[] compress(Compression compression, byte[] rawValue, int offset, int length) throws IOException {
		Compressor	compressor;
		
		compressor = CodecProvider.getCompressor(compression);
		if (compressor == null) {
			compressor = CodecProvider.nullCodec;
		}
		return compressor.compress(rawValue, offset, length);
	}   
	
	public static byte[] decompress(Compression compression, byte[] value, int offset, int length, int uncompressedLength) throws IOException {
		Decompressor	decompressor;
		
		decompressor = CodecProvider.getDecompressor(compression);
		if (decompressor == null) {
			decompressor = CodecProvider.nullCodec;
		}
		return decompressor.decompress(value, offset, length, uncompressedLength);
	}
}
