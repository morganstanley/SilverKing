package com.ms.silverking.fs.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class RewriteTest {
	private RandomAccessFile	raf;
	
	private static final int	blockSize = 262144;
	private static final byte[][]	buf = new byte[26][blockSize];
	private static final byte[]	buf1 = new byte[blockSize];
	private static final byte[]	buf2 = new byte[blockSize];
	
	private enum Test {Current, Past, CurrentAndPast, CurrentAndPastAndExtension, Random};
	private enum Mode {Write, Verify};
	
	private static void fillBuf(byte[] buf, int offset, int length, byte b) {
		for (int i = 0; i < length; i++) {
			buf[offset + i] = b;
		}
	}
	
	static {
		fillBuf(buf1, 0, buf1.length, (byte)'A');
		fillBuf(buf2, 0, buf2.length, (byte)'B');
		for (int i = 0; i < 26; i++) {
			fillBuf(buf[i], 0, buf[i].length, (byte)('A' + i));
		}
	}
	
	public RewriteTest(File file, Mode mode) throws FileNotFoundException {
		raf = new RandomAccessFile(file, mode == Mode.Write ? "rw" : "r");
	}
	
	private void testCurrent() throws IOException {
		raf.write(buf1, 0, 128);
		raf.seek(64);
		raf.write(buf2, 0, 32);
	}

	private void testPast() throws IOException {
		raf.write(buf1, 0, blockSize);
		raf.write(buf1, 0, blockSize);
		raf.write(buf1, 0, blockSize);
		raf.seek(64);
		raf.write(buf2, 0, blockSize);
	}
	
	private void testCurrentAndPast() throws IOException {
		raf.write(buf1, 0, blockSize);
		raf.write(buf1, 0, 128);
		raf.seek(64);
		raf.write(buf2, 0, blockSize);
	}
	
	private void testCurrentAndPastAndExtension() throws IOException {
		raf.write(buf1, 0, blockSize);
		raf.write(buf1, 0, 64);
		raf.seek(128);
		raf.write(buf2, 0, blockSize);
	}
	
	private void testRandom(int numWrites, Mode mode) throws IOException {
		Random	r;
		Stopwatch	sw;
		long	fileLength = blockSize * 100;
		int		maxSize = blockSize;
		//long	fileLength = blockSize * 2;
		//int		maxSize = 24;
		byte[]	readBuf;
		
		readBuf = new byte[blockSize];
		r = new Random(0);
		sw = new SimpleStopwatch();
		for (int i = 0; i < numWrites; i++) {
			long	offset;
			int		size;
			
			offset = r.nextLong() % fileLength;
			if (offset < 0) {
				offset = -offset;
			}
			size = r.nextInt(maxSize - 1) + 1;
			System.out.printf("%d %d\n", offset, size);
			raf.seek(offset);
			if (mode == Mode.Write) {
				raf.write(buf[i % 26], 0, size);
			} else {
				int	numRead;
				
				// Note - this verification only works for cases where writes
				// don't overlap
				numRead = raf.read(readBuf, 0, size);
				if (numRead != size) {
					throw new RuntimeException("numRead != size");
				}
				for (int j = 0; j < size; j++) {
					if (readBuf[j] != buf[i % 26][j]) {
						System.out.printf("%d\t%x != %x\n", offset + j, readBuf[j], buf[i % 26][j]);
					}
				}
			}
		}
		sw.stop();
		System.out.printf("Elapsed: %s\n", sw.getElapsedSeconds());
	}
	
	public static void main(String[] args) {
		if (args.length < 2 || args.length > 4) {
			System.err.println("args: <file> <test> [numWrites] [mode]");
		} else {
			try {
				RewriteTest	rt;
				String	file;
				Test	test;
				Mode	mode;

				if (args.length == 4) {
					mode = Mode.valueOf(args[3]);
				} else {
					mode = Mode.Write;
				}
				file = args[0];
				test = Test.valueOf(args[1]);
				rt = new RewriteTest(new File(file), mode);
				switch (test) {
				case Current:
					rt.testCurrent();
					break;
				case Past:
					rt.testPast();
					break;
				case CurrentAndPast:
					rt.testCurrentAndPast();
					break;
				case CurrentAndPastAndExtension:
					rt.testCurrentAndPastAndExtension();
					break;
				case Random:
					if (args.length != 4) {
						System.err.println("Random requires numWrites and mode");
					} else {
						int	numWrites;
						
						numWrites = Integer.parseInt(args[2]);
						rt.testRandom(numWrites, mode);
					}
					break;
				default: throw new RuntimeException("panic");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
