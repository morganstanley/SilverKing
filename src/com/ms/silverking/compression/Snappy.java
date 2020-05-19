package com.ms.silverking.compression;

import java.io.IOException;

import com.ms.silverking.text.StringUtil;

public class Snappy implements Compressor, Decompressor {
  private static final int snappyInitFactor = 10;

  public Snappy() {
  }

  public byte[] compress(byte[] rawValue, int offset, int length) throws IOException {
        /*
        SnappyCompressor    sc;
        byte[]                output;
        
        sc = new SnappyCompressor();
        sc.setInput(rawValue, offset, length);
        output = new byte[length];
        sc.compress(output, 0, length);
        return output;
        */
    return rawValue;
  }

  public byte[] decompress(byte[] value, int offset, int length, int uncompressedLength) throws IOException {
        /*
        SnappyDecompressor    sd;
        byte[]    output;
        
        sd = new SnappyDecompressor();
        sd.setInput(value, offset, length);
        output = new byte[length];
        sd.decompress(output, 0, length);
        return output;
        */
    return value;
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      for (String arg : args) {
        byte[] original;
        byte[] compressed;
        byte[] uncompressed;
        original = arg.getBytes();
        compressed = new Snappy().compress(original, 0, original.length);
        uncompressed = new Snappy().decompress(compressed, 0, compressed.length, original.length);
        //print of uncompressed may be 'corrupted' by non-printable chars from MD5
        System.out.println(arg + "\t" + original.length + "\t" + compressed.length + "\t" + new String(uncompressed));
        System.out.println(StringUtil.byteArrayToHexString(original));
        System.out.println(StringUtil.byteArrayToHexString(uncompressed));
        //int len = uncompressed.length - MD5Hash.MD5_BYTES;
        //byte[] noMd5 = new byte[len];
        //System.arraycopy(uncompressed, 0, noMd5, 0, len);
        //System.out.println(StringUtil.byteArrayToHexString(noMd5));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
