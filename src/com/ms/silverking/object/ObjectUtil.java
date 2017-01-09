// ObjectUtil.java


package com.ms.silverking.object;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

/**
 * Object serialization utilities. 
 */
public class ObjectUtil {
	public static byte[] objToBytes(Object obj) throws IOException {
		return objToBytes(obj, false);
	}
	
	public static byte[] objToBytesWithSize(Object obj) throws IOException {
		return objToBytes(obj, true);
	}
	
	private static byte[] objToBytes(Object obj, boolean includeSize) throws IOException {
		ByteArrayOutputStream	bos;
		ObjectOutputStream		oos;
		byte []					data;
        
        if (obj instanceof byte[] && !includeSize) {
        	return (byte[]) obj;
        }
		bos = new ByteArrayOutputStream(); 
		oos = new ObjectOutputStream(bos);
		if (includeSize) {
			oos.writeInt(-1); // pad with bogus length, real length written below
		}
		oos.writeObject(obj);
		oos.flush(); 
		oos.close(); 
		bos.close();
		data = bos.toByteArray();
		if (includeSize) {
			NumConversion.intToBytes(data.length - 4, data);
		}
		return data;
	}	
	
	public static Object bytesToObj(byte[] data) throws IOException, ClassNotFoundException {
		return bytesToObj(data, 0, data.length);
	}
	
	public static Object bytesToObj(byte[] data, int offset) throws IOException, 
		ClassNotFoundException {
		return bytesToObj(data, offset, data.length - offset);
	}
	
	public static Object bytesToObj(byte[] data, int offset, int length) throws IOException, 
											ClassNotFoundException {
		ByteArrayInputStream	bis;
		ObjectInputStream		ois;
		Object					object;

		bis = new ByteArrayInputStream(data, offset, length); 
		ois = new ObjectInputStream(bis); 
		object = ois.readObject();
		ois.close(); 
		bis.close();
		return object;
	}
		
	public static Object bytesToObjIgnoreSize(byte[] data) throws IOException, ClassNotFoundException {
		ByteArrayInputStream bis;
		ObjectInputStream ois;
		Object object;

		bis = new ByteArrayInputStream(data);
		ois = new ObjectInputStream(bis);
		ois.readInt();
		object = ois.readObject();
		ois.close();
		bis.close();
		return object;
	}
	
	public static void main(String[] args) {
		try {
			String		testString;
			int			reps;
			Stopwatch	sw;
			String		s;
			byte[]		b;
			
			if (args.length < 2) {
				System.out.println("<string> <reps>");
				return;
			}
			testString = args[0];
			reps = Integer.parseInt(args[1]);
			sw = new SimpleStopwatch();
			s = null;
			b = null;
			for (int i = 0; i < reps; i++) {				
				b = objToBytes(testString);
				s = (String)bytesToObj(b);
			}
			sw.stop();
			if (!s.equals(testString)) {
				throw new RuntimeException("verify failed");
			}
			System.out.println(sw.getElapsedSeconds() / (double)reps);

			System.out.println(b.length);
			b = objToBytesWithSize(testString);
			System.out.println(b.length);
			s = (String)bytesToObjIgnoreSize(b);
			if (!s.equals(testString)) {
				throw new RuntimeException("verify failed");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
