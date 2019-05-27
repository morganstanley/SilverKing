package com.ms.silverking.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;

import org.apache.hadoop.io.compress.bzip2.CBZip2InputStream;
import org.apache.hadoop.io.compress.bzip2.CBZip2OutputStream;

import com.ms.silverking.log.Log;
import com.ms.silverking.text.StringUtil;

public class BZip2 implements Compressor, Decompressor {
    private static final int   bzip2InitFactor = 10;	
    
    public BZip2() {
    }
    
	public byte[] compress(byte[] rawValue, int offset, int length) throws IOException {
	    ByteArrayOutputStream  baos;
	    CBZip2OutputStream     bzip2os;
	    byte[] buf;
	    int    compressedLength;

        //Log.warning("rawValue.length ", rawValue.length);
	    baos = new ByteArrayOutputStream(rawValue.length / bzip2InitFactor);
	    baos.write(0x42);
        baos.write(0x5a);
	    bzip2os = new CBZip2OutputStream(baos);
	    bzip2os.write(rawValue, offset, length);
        bzip2os.flush();
	    bzip2os.close();
	    
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
        //System.arraycopy(md5, 0, buf, compressedLength, md5.length);
	    
        //Log.warning("buf.length ", buf.length);
        //System.out.println("\t"+ StringUtil.byteArrayToHexString(buf));
	    return buf;
	}

	public byte[] decompress(byte[] value, int offset, int length, int uncompressedLength) throws IOException {
		CBZip2InputStream	bzip2is;
		InputStream 		inStream;
		byte[]				uncompressedValue;
		
		//System.out.println(value.length +" "+ offset +" "+ length);
		//System.out.println(StringUtil.byteArrayToHexString(value, offset, length));
		uncompressedValue = new byte[uncompressedLength];		
		inStream = new ByteArrayInputStream(value, offset, length);
		try {
			int		b;
			
			b = inStream.read();
			if (b != 'B') {
				throw new IOException("Invalid bzip2 value");
			}
			b = inStream.read();
			if (b != 'Z') {
				throw new IOException("Invalid bzip2 value");
			}
			bzip2is = new CBZip2InputStream(inStream);
			try {
				int	totalRead;
				
				totalRead = 0;
				do {
					int numRead;
					
					numRead = bzip2is.read(uncompressedValue, totalRead, uncompressedLength - totalRead);
					if (numRead < 0) {
						throw new RuntimeException("panic");
					}
					totalRead += numRead;
				} while (totalRead < uncompressedLength);
				return uncompressedValue;
			} finally {
				bzip2is.close();
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
				compressed = new BZip2().compress(original, 0, 0);
				uncompressed = new BZip2().decompress(compressed, 0, 0, original.length);
				System.out.println(arg +"\t"+ original.length +"\t"+ compressed.length +"\t"+ new String(uncompressed));
				System.out.println(StringUtil.byteArrayToHexString(original));
				System.out.println(StringUtil.byteArrayToHexString(uncompressed));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
