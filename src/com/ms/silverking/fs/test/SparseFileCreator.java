package com.ms.silverking.fs.test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

public class SparseFileCreator {
	private static final int	bufLength = 1024 * 1024;
	private static final byte[]	buf = new byte[bufLength];
	
	static {
		new Random(0).nextBytes(buf);
	}
	
	private void write(RandomAccessFile raf, int length) throws IOException {
		int	bytesToWrite;
		int	totalWritten;
		
		totalWritten = 0;
		while (totalWritten < length) {
			bytesToWrite = Math.min(buf.length, length - totalWritten);
			raf.write(buf, 0, bytesToWrite);
			totalWritten += bytesToWrite;
		}
	}

	public void createSparseFile(File f, int headerLength, int skipLength, int tailLength, int finalLength) throws IOException {
		RandomAccessFile	raf;
		
		raf = new RandomAccessFile(f, "rw");
		write(raf, headerLength);
		raf.seek(raf.getFilePointer() + skipLength);
		write(raf, tailLength);
		if (finalLength > 0) {
			raf.setLength(finalLength);
		}
	}
	
	public static void main(String[] args) {
		if (args.length != 4 && args.length != 5) {
			System.err.println("args: <file> <headerLength> <skipLength> <tailLength> [finalLength]");
		} else {
			try {
				SparseFileCreator	sfc;
				String	file;
				int		headerLength;
				int		skipLength;
				int		tailLength;
				int		finalLength;
				
				file = args[0];
				headerLength = Integer.parseInt(args[1]);
				skipLength = Integer.parseInt(args[2]);
				tailLength = Integer.parseInt(args[3]);
				if (args.length == 5) {
					finalLength = Integer.parseInt(args[4]);				
				} else {
					finalLength = 0;
				}
				sfc = new SparseFileCreator();
				sfc.createSparseFile(new File(file), headerLength, skipLength, tailLength, finalLength);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
