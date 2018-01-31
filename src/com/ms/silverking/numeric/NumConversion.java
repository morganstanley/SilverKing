package com.ms.silverking.numeric;

import java.util.concurrent.TimeUnit;

import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.SimpleTimer;


public class NumConversion {
	public static final int	BYTES_PER_FLOAT = 4;
	public static final int	BYTES_PER_DOUBLE = 8;
	public static final int	BYTES_PER_CHAR = 2;
	public static final int	BYTES_PER_SHORT = 2;
	public static final int	BYTES_PER_INT = 4;
	public static final int	BYTES_PER_LONG = 8;
    public static final int BYTE_MAX_UNSIGNED_VALUE = 255;
	
    //////////
    // boolean (here for convenience)
    
    public static boolean byteToBoolean(byte b0) {
        return b0 != 0;
    }

    public static boolean byteToBoolean(byte[] b) {
        return byteToBoolean(b[0]);
    }

    public static boolean byteToBoolean(byte[] b, int offset) {
        return byteToBoolean(b[offset]);
    }

    public static byte booleanToByte(boolean b) {
        return b ? (byte)1 : (byte)0;
    }
    
    //////////
    /// byte
    // other byte cases are too trivial to be useful
    
    public static byte[] byteToBytes(byte value) {
        byte[]  b;
        
        b = new byte[1];
        b[0] = value;
        return b;
    }
	
    //////////
    // short
	
    public static short bytesToShort(byte b0, byte b1) {
        short value;
        
        value = (short)(((b0 << 8) & 0x0000ff00) | ((b1) & 0x000000ff));
        return value;
    }

    public static short bytesToShort(byte[] b) {
        return bytesToShort(b, 0);
    }

    public static short bytesToShort(byte[] b, int offset) {
        return bytesToShort(b[offset + 0], b[offset + 1]);
    }

    public static short bytesToShortLittleEndian(byte[] b) {
        return bytesToShortLittleEndian(b, 0);
    }
    
    public static short bytesToShortLittleEndian(byte[] b, int offset) {
        return bytesToShort(b[offset + 1], b[offset + 0]);
    }
    
    public static byte[] shortToBytes(short value) {
        byte[]  b;
        
        b = new byte[BYTES_PER_SHORT];
        shortToBytes(value, b, 0);
        return b;
    }
    
    public static void shortToBytes(short value, byte[] b) {
        shortToBytes(value, b, 0);
    }
    
    public static void shortToBytes(short value, byte[] b, int offset) {
        b[offset + 0] = (byte)(value >> 8 & 0xff);
        b[offset + 1] = (byte)(value & 0xff);
    }   
    
    public static void shortToBytesLittleEndian(short value, byte[] b, int offset) {
        b[offset + 1] = (byte)(value >> 8 & 0xff);
        b[offset + 0] = (byte)(value & 0xff);
    }
    
    ///////////////////
    // unsigned short
	
	public class UnsignedShort {
	    public static final int MAX_VALUE = 0xffff;
        public static final int MIN_VALUE = 0;
	}
    
    public static int bytesToUnsignedShort(byte[] b, int offset) {
        return bytesToUnsignedShort(b[offset + 0], b[offset + 1]);
    }
    
    public static int bytesToUnsignedShort(byte b0, byte b1) {
        int value;
        
        value = ((b0 << 8) & 0x0000ff00) | ((b1) & 0x000000ff);
        return value;
    }
    
    public static void unsignedShortToBytes(int value, byte[] b, int offset) {
        assert value >= UnsignedShort.MIN_VALUE && value <= UnsignedShort.MAX_VALUE;
        b[offset + 0] = (byte)(value >> 8 & 0xff);
        b[offset + 1] = (byte)(value & 0xff);
    }   
    
    ////////
    // int
    
    public static int bytesToInt(byte b0, byte b1, byte b2, byte b3) {
        int value;
        
        value = ((b0 << 24) & 0xff000000) 
              | ((b1 << 16) & 0x00ff0000) 
              | ((b2 << 8)  & 0x0000ff00) 
              | ((b3)       & 0x000000ff);
        return value;
    }
    
	public static int bytesToInt(byte[] b) {
		return bytesToInt(b[0], b[1], b[2], b[3]);
	}

	public static int bytesToInt(byte[] b, int offset) {
		return bytesToInt(b[offset + 0], b[offset + 1], 
						b[offset + 2], b[offset + 3]);
	}
	
	public static int bytesToIntLittleEndian(byte[] b) {
		return bytesToIntLittleEndian(b, 0);
	}
	
	public static int bytesToIntLittleEndian(byte[] b, int offset) {
		return bytesToInt(b[offset + 3], b[offset + 2], 
						b[offset + 1], b[offset + 0]);
	}
	
    public static void intToBytesLittleEndian(int value, byte[] b, int offset) {
		b[offset + 3] = (byte)(value >>> 24);
		b[offset + 2] = (byte)(value >>  16 & 0xff);
		b[offset + 1] = (byte)(value >>   8 & 0xff);
		b[offset + 0] = (byte)(value        & 0xff);
    }	
    
    public static void intToBytes(int value, byte[] b, int offset) {
		b[offset + 0] = (byte)(value >>> 24);
		b[offset + 1] = (byte)(value >>  16 & 0xff);
		b[offset + 2] = (byte)(value >>   8 & 0xff);
		b[offset + 3] = (byte)(value        & 0xff);
    }	
	
    public static void intToBytes(int value, byte[] b) {
		intToBytes(value, b, 0);
	}
    
    public static byte[] intToBytes(int value) {
    	byte[]	b;
    	
    	b = new byte[BYTES_PER_INT];
    	intToBytes(value, b);
    	return b;
    }
    
    public static int intSwapEndian(int value) {
		return bytesToInt((byte)(value >>> 24),
						(byte)(value >> 16 & 0xff),
						(byte)(value >> 8 & 0xff),
						(byte)(value & 0xff));
    }    
    
    /**
     * Return a byte to an integer in the range [0, 255]
     * @return
     */
    public static int byteToPositiveInt(byte b) {
    	return ((int)b) & 0xff;
    }
    
    public static int unsignedByteToInt(byte b) {
    	return ((int)b) & 0xff;
    }
	
    public static int unsignedByteToInt(byte[] b, int offset) {
    	return unsignedByteToInt(b[offset]);
    }
    
    ////////
    // long
	
	public static long bytesToLong(byte b0, byte b1, byte b2, byte b3, 
	                            byte b4, byte b5, byte b6, byte b7) {
        long    value;
		
		value =   (((long)b0 << 56) & 0xff00000000000000L) 
                | (((long)b1 << 48) & 0x00ff000000000000L) 
                | (((long)b2 << 40) & 0x0000ff0000000000L) 
                | (((long)b3 << 32) & 0x000000ff00000000L) 
                | (((long)b4 << 24) & 0x00000000ff000000L) 
    			| (((long)b5 << 16) & 0x0000000000ff0000L) 
    			| (((long)b6 << 8)  & 0x000000000000ff00L) 
    			| (((long)b7)       & 0x00000000000000ffL);
		return value;
	}
	
    public static long bytesToLong(byte[] b) {
        return bytesToLong(b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]);
    }
	
    public static long bytesToLong2(byte[] b) {
        return bytesToLong(b, 0);
    }
        
    public static long bytesToLong(byte[] b, int offset) {
        return bytesToLong(b[offset + 0], b[offset + 1], b[offset + 2], b[offset + 3], 
                b[offset + 4], b[offset + 5], b[offset + 6], b[offset + 7]);
    }
    
    public static long bytesToLongLittleEndian(byte[] b) {
        return bytesToLong(b[7], b[6], b[5], b[4], b[3], b[2], b[1], b[0]);
    }
    
    public static long bytesToLongLittleEndian(byte[] b, int offset) {
        return bytesToLong(b[offset + 7], b[offset + 6], b[offset + 5], b[offset + 4], 
                b[offset + 3], b[offset + 2], b[offset + 1], b[offset + 0]);
    }
    
    public static void longToBytes(long value, byte[] b, int offset) {
        b[offset + 0] = (byte)(value >> 56 & 0xff);
        b[offset + 1] = (byte)(value >> 48 & 0xff);
        b[offset + 2] = (byte)(value >> 40 & 0xff);
        b[offset + 3] = (byte)(value >> 32 & 0xff);
        b[offset + 4] = (byte)(value >> 24 & 0xff);
        b[offset + 5] = (byte)(value >> 16 & 0xff);
        b[offset + 6] = (byte)(value >> 8  & 0xff);
        b[offset + 7] = (byte)(value       & 0xff);
    }   
    
    public static void longToBytesLittleEndian(long value, byte[] b, int offset) {
        b[offset + 7] = (byte)(value >> 56 & 0xff);
        b[offset + 6] = (byte)(value >> 48 & 0xff);
        b[offset + 5] = (byte)(value >> 40 & 0xff);
        b[offset + 4] = (byte)(value >> 32 & 0xff);
        b[offset + 3] = (byte)(value >> 24 & 0xff);
        b[offset + 2] = (byte)(value >> 16 & 0xff);
        b[offset + 1] = (byte)(value >> 8  & 0xff);
        b[offset + 0] = (byte)(value       & 0xff);
    }   
    
    public static void longToBytes(long value, byte[] b) {
    	longToBytes(value, b, 0);
    }
    
    public static byte[] longToBytes(long value) {
        byte[]  b;
        
        b = new byte[BYTES_PER_LONG];
        longToBytes(value, b, 0);
        return b;
    }
    
    ////////
    // double
    
    public static double bytesToDouble(byte[] b) {
        return bytesToDouble(b, 0);
    }
    
    public static double bytesToDouble(byte[] b, int offset) {
        return Double.longBitsToDouble(bytesToLong(b, offset));
    }
    
    public static byte[] doubleToBytes(double value) {
        return longToBytes(Double.doubleToLongBits(value));
    }   
    
    public static void doubleToBytes(long value, byte[] b) {
        doubleToBytes(value, b, 0);
    }   
    
    public static void doubleToBytes(long value, byte[] b, int offset) {
        longToBytes(Double.doubleToLongBits(value), b, offset);
    }  
    
    /////////////
    
    public static long intsToLong(int msi, int lsi) {
        return ((long)msi << 32) | ((long)lsi & 0xffffffffL);
    }
    
    public static long parseHexStringAsUnsignedLong(String s) {
        return Long.parseUnsignedLong(s, 16);
    }
    
    /////////////
        
	public static void main(String[] args) {
		int	i;
		byte[]	b;
		
//		i = Integer.parseInt(args[0]);
//		b = new byte[BYTES_PER_INT];
//		intToBytes(i, b);
//		System.out.println( ">>"+ bytesToInt(b) );

		
		// benchmark test
		byte[] bytes = {Byte.MIN_VALUE, Byte.MAX_VALUE, Byte.MIN_VALUE, Byte.MAX_VALUE, Byte.MIN_VALUE, Byte.MAX_VALUE, Byte.MIN_VALUE, Byte.MAX_VALUE};

		SimpleTimer timer = new SimpleTimer(TimeUnit.SECONDS, 3);
		timer.reset();
		timer.stop();
		timer.start();
		while (!timer.hasExpired())
			System.out.println(timer.getSplitSeconds());
//		
////		timer = new SimpleTimer(TimeUnit.SECONDS, 3);
////		timer.reset();
////		while (!timer.hasExpired())
////			System.out.println(timer.getSplitSeconds());
//
		SimpleStopwatch watch = new SimpleStopwatch();
		watch.reset();
		for (int j = 0; j < 1_000_000_000; j++)
			bytesToLong(bytes);
		watch.stop();
		
		System.out.println("Time for  inline: " + watch.getElapsedMillis());
		
		watch.reset();
		for (int j = 0; j < 1_000_000_000; j++)
			bytesToLong2(bytes);
		watch.stop();
		
		System.out.println("Time for wrapped: " + watch.getElapsedMillis());
		
//		SimpleTimer timer = new SimpleTimer(TimeUnit.MILLISECONDS, 1_000_000_000);
//		timer.stop();
////		timer.reset();
//		timer.start();
//		for (int j = 0; j < 1_000_000; j++)
//			bytesToLong(bytes);
//		timer.stop();
//
//		System.out.println("Time for  inline: " + timer.getElapsedMillis());
//		
//		timer.start();
//		for (int j = 0; j < 1_000_000; j++)
//			bytesToLong2(bytes);
//		timer.stop();
//		
//		System.out.println("Time for wrapped: " + timer.getElapsedMillis());
	}
}
