package com.ms.silverking.fs.test;

import static com.ms.silverking.fs.TestUtil.setupAndCheckTestsDirectory;
import static com.ms.silverking.io.FileUtil.copyDirectories;

import java.io.File;
import java.net.URISyntaxException;
import java.security.CodeSource;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ms.silverking.fs.TestUtil;
import com.ms.silverking.fs.test.FSStress;
import com.ms.silverking.testing.Util;
import com.ms.silverking.testing.annotations.SkfsLarge;

@SkfsLarge
public class FSStressTest {

	private static String testsDirPath;

	static {
		testsDirPath = TestUtil.getTestsDir();
	}
	
	private static final String fsStressDirName = "fs-stress";
	private final static File fsStressDir = new File(testsDirPath, fsStressDirName);

	@BeforeClass
	public static void setUpBeforeClass() {
		setupAndCheckTestsDirectory(fsStressDir);
	}
	
	// jarFile is the root folder location (start of classpath), we want to copy our entire repo into skfs, and run the stress test
	@Test
	public void testStress() throws URISyntaxException {
		CodeSource codeSource = getClass().getProtectionDomain().getCodeSource();
		File jarFile = new File(codeSource.getLocation().toURI().getPath());
//		System.out.println(jarFile);
		
		copyDirectories(jarFile, fsStressDir);

		FSStress fsStress = new FSStress(fsStressDir, 10, true);
		fsStress.stressMultiple(10);
	}
	
	public static void main(String[] args) {
		Util.runTests(FSStressTest.class);
	}
}
