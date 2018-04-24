package com.ms.silverking.fs.test;

import static com.ms.silverking.fs.TestUtil.setupAndCheckTestsDirectory;
import static com.ms.silverking.fs.test.RewriteTest.Mode.Verify;
import static com.ms.silverking.fs.test.RewriteTest.Mode.Write;
import static com.ms.silverking.fs.test.RewriteTest.blockSize;
import static com.ms.silverking.fs.test.RewriteTest.fillBuf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ms.silverking.fs.TestUtil;
import com.ms.silverking.fs.test.RewriteTest.Mode;
import com.ms.silverking.testing.Util;
import com.ms.silverking.testing.annotations.SkfsSmall;

@SkfsSmall
public class RewriteTestTest {

	private static String testsDirPath;

	static {
		testsDirPath = TestUtil.getTestsDir();
	}
	
	private static final String rewriteDirName = "re-write";
	private final static File rewriteDir = new File(testsDirPath, rewriteDirName);

	private RewriteTest rt;
	private byte byte_A = 'A';
	private byte byte_B = 'B';
	private byte byte_1 = '1';
	private byte byte_2 = '2';
	private byte byte_3 = '3';
	private byte byte_4 = '4';
	private byte byte_5 = '5';
	private byte byte_6 = '6';
	private Mode[] testCases = new Mode[]{Write, Verify};
	
	@BeforeClass
	public static void setUpBeforeClass() {
		setupAndCheckTestsDirectory(rewriteDir);
	}
	
	@Test
	public void testCurrent() throws IOException {
		String testName = "Current";
		
		rt = createRewrite(testName, Write);
		rt.testCurrent();

		rt = createRewrite(testName, Verify);
		int size = 128;
		int lastStart = 64+32;
		int lastLength = size -lastStart;
		byte[] buf = new byte[128];
		fillBuf(buf,         0,         64, byte_A);
		fillBuf(buf,        64,         32, byte_B);
		fillBuf(buf, lastStart, lastLength, byte_A);
		checkRead(buf);
	}
	
	@Test
	public void testPast() throws IOException {
		String testName = "Past";
		
		rt = createRewrite(testName, Write);
		rt.testPast();

		rt = createRewrite(testName, Verify);
		int size = blockSize*3;
		int lastStart = 64+blockSize;
		int lastLength = size -lastStart;
		byte[] buf = new byte[size];
		fillBuf(buf,         0,         64, byte_A);
		fillBuf(buf,        64,  blockSize, byte_B);
		fillBuf(buf, lastStart, lastLength, byte_A);
		checkRead(buf);
	}
	
	@Test
	public void testCurrentAndPast() throws IOException {
		String testName = "CurrentAndPast";
		
		rt = createRewrite(testName, Write);
		rt.testCurrentAndPast();

		rt = createRewrite(testName, Verify);
		int size = blockSize+128;
		int lastStart = 64+blockSize;
		int lastLength = size -lastStart;
		byte[] buf = new byte[size];
		fillBuf(buf,         0,         64, byte_A);
		fillBuf(buf,        64,  blockSize, byte_B);
		fillBuf(buf, lastStart, lastLength, byte_A);
		checkRead(buf);
	}
	
	@Test
	public void testCurrentAndPastAndExtension() throws IOException {
		String testName = "CurrentAndPastAndExtension";
		
		rt = createRewrite(testName, Write);
		rt.testCurrentAndPastAndExtension();

		rt = createRewrite(testName, Verify);
		int size = blockSize+128;
		byte[] buf = new byte[size];
		fillBuf(buf,         0,        128, byte_A);
		fillBuf(buf,       128,  blockSize, byte_B);
		checkRead(buf);
	}
	
	@Test
	public void testLast() throws IOException {
		String testName = "Last";
		
		rt = createRewrite(testName, Write);
		rt.testLast();

		rt = createRewrite(testName, Verify);
		int size = blockSize+128;
		byte[] buf = new byte[size];
		fillBuf(buf,            0,   blockSize+64, byte_A);
		fillBuf(buf, blockSize+64,             64, byte_B);
		checkRead(buf);
	}
	
	@Test
	public void testLastAndExtension() throws IOException {
		String testName = "LastAndExtension";
		
		rt = createRewrite(testName, Write);
		rt.testLastAndExtension();

		rt = createRewrite(testName, Verify);
		int size = blockSize+64+256;
		byte[] buf = new byte[size];
		fillBuf(buf,            0,   blockSize+64, byte_A);
		fillBuf(buf, blockSize+64,            256, byte_B);
		checkRead(buf);
	}
	
	@Test
	public void testBlockBorderOneByteEachSide() throws IOException {
		String testName = "BlockBorderOneByteEachSide";
		rt = createRewrite(testName, Write);
		rt.testBlockBorderOneByteEachSide();
		
		rt = createRewrite(testName, Verify);
		int size = blockSize*2;
		byte[] buf = new byte[size];
		fillBuf(buf,           0, blockSize-1, byte_A);
		fillBuf(buf, blockSize-1,           1, byte_1);
		fillBuf(buf,   blockSize,           1, byte_2);
		fillBuf(buf, blockSize+1, blockSize-1, byte_B);
		checkRead(buf); 
	}
	
	@Test
	public void testBlockBorder() throws IOException {
		String testName = "BlockBorder";
		rt = createRewrite(testName, Write);
		rt.testBlockBorder();
		
		rt = createRewrite(testName, Verify);
		int size = blockSize*2;
		byte[] buf = new byte[size];
		fillBuf(buf,           0, blockSize-3, byte_A);
		fillBuf(buf, blockSize-3,           1, byte_1);
		fillBuf(buf, blockSize-2,           1, byte_2);
		fillBuf(buf, blockSize-1,           1, byte_3);
		fillBuf(buf,   blockSize,           1, byte_4);
		fillBuf(buf, blockSize+1,           1, byte_5);
		fillBuf(buf, blockSize+2,           1, byte_6);
		fillBuf(buf, blockSize+3, blockSize-3, byte_B);
		checkRead(buf); 
	}
	
	@Test
	public void testRandom() throws IOException {
		for (Mode testCase : testCases) {
			rt = createRewrite("Random", testCase);
			rt.testRandom(10, testCase);
		}
	}
	
	private void checkRead(byte[] expected) throws IOException {
		RandomAccessFile raf = rt.getFile();
		byte[] actual = new byte[(int)raf.length()];
		int x = raf.read(actual);
		
		x = raf.read();	// read past last byte, so EOF
		assertEquals(-1, x);
		
		assertArrayEquals(expected, actual);
	}
	
	private RewriteTest createRewrite(String name, Mode m) throws FileNotFoundException {
		return new RewriteTest(new File(rewriteDir, "test"+name), m);
	}
	
	public static void main(String[] args) {
		Util.runTests(RewriteTestTest.class);
	}

}
