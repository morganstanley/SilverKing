package com.ms.silverking.fs.test;

import static com.ms.silverking.fs.TestUtil.setupAndCheckTestsDirectory;

import java.io.File;
import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ms.silverking.fs.TestUtil;
import com.ms.silverking.fs.test.SparseFileCreator;
import com.ms.silverking.testing.Util;
import com.ms.silverking.testing.annotations.SkfsSmall;

@SkfsSmall
public class SparseFileCreatorTest {

	private static String testsDirPath;

	static {
		testsDirPath = TestUtil.getTestsDir();
	}
	
	private static final String sfcDirName = "sparse-file-creator";
	private final static File sfcDir = new File(testsDirPath, sfcDirName);

	@BeforeClass
	public static void setUpBeforeClass() {
		setupAndCheckTestsDirectory(sfcDir);
	}
	
	@Test
	public void testStress() throws IOException {
		int[][] testCases = {
			{0,       0,       0,       0},
			{0,       0,       0,       1},
			{100,     1000,    1,       0},
			{100_000, 100_000, 100_000, 0},
		};
		
		int count = 1;
		for (int[] testCase : testCases) {
			int headerLength = testCase[0];
			int skipLength   = testCase[1];
			int tailLength   = testCase[2];
			int finalLength  = testCase[3];
			
			SparseFileCreator sfc = new SparseFileCreator();
			sfc.createSparseFile(new File(sfcDir, "file" + count++), headerLength, skipLength, tailLength, finalLength);
		}
	}
	
	public static void main(String[] args) {
		Util.runTests(SparseFileCreatorTest.class);
	}

}
