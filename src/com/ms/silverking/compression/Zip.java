package com.ms.silverking.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import com.ms.silverking.log.Log;
import com.ms.silverking.text.StringUtil;

public class Zip implements Compressor, Decompressor {
    private static final int   zipInitFactor = 10;	

	public byte[] compress(byte[] rawValue, int offset, int length) throws IOException {
	    ByteArrayOutputStream 	baos;
	    DeflaterOutputStream	zipos;
	    byte[] buf;
	    int    compressedLength;
        	    
        //Log.warning("rawValue.length ", rawValue.length);
	    baos = new ByteArrayOutputStream(rawValue.length / zipInitFactor);
	    zipos = new DeflaterOutputStream(baos);
	    zipos.write(rawValue, offset, length);
	    zipos.flush();
	    zipos.close();
	    
        baos.flush();
        baos.close();
        buf = baos.toByteArray();
        //System.out.println(StringUtil.byteArrayToHexString(buf));
        
        compressedLength = buf.length;
        //Log.warning("compressedLength ", compressedLength);
        
        if (Log.levelMet(Level.FINE)) {
            Log.fine("rawValue.length: "+ rawValue.length);
            Log.fine("buf.length: "+ buf.length);
            Log.fine("compressedLength: "+ compressedLength);
        }	    
        //Log.warning("buf.length ", buf.length);
	    return buf;
	}
	
	public byte[] decompress(byte[] value, int offset, int length, int uncompressedLength) throws IOException {
		InflaterInputStream	zipis;
		InputStream 		inStream;
		byte[]				uncompressedValue;
		
		uncompressedValue = new byte[uncompressedLength];		
		inStream = new ByteArrayInputStream(value, offset, length);
		try {
			zipis = new InflaterInputStream(inStream);
			try {
				int	totalRead;
				
				totalRead = 0;
				do {
					int numRead;
					
					numRead = zipis.read(uncompressedValue, totalRead, uncompressedLength - totalRead);
					if (numRead < 0) {
						throw new RuntimeException("panic");
					}
					totalRead += numRead;
				} while (totalRead < uncompressedLength);
				return uncompressedValue;
			} finally {
				zipis.close();
			}
		} finally {
			inStream.close();
		}
	}
	
	// for unit testing only
	public static void main(String[] args) {
		try {
			for (String arg : args) {
				byte[]	original;
				byte[]	compressed;
				byte[]	uncompressed;
				
				original = arg.getBytes();
				compressed = new Zip().compress(original, 0, 0);
				uncompressed = new Zip().decompress(compressed, 0, 0, original.length);
				System.out.println(arg +"\t"+ original.length +"\t"+ compressed.length +"\t"+ new String(uncompressed));
				System.out.println(StringUtil.byteArrayToHexString(original));
				System.out.println(StringUtil.byteArrayToHexString(uncompressed));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
