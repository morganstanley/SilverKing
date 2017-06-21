package com.ms.silverking.fs.test;

import static org.junit.Assert.*;
import static com.ms.silverking.fs.TestUtil.setupAndCheckTestsDirectory;

import java.io.File;
import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ms.silverking.process.ProcessExecutor;
import com.ms.silverking.testing.Util;
import com.ms.silverking.testing.annotations.SkfsLarge;
import com.ms.silverking.fs.TestUtil;

@SkfsLarge
public class FileWriteWithDelayTest {

	private static String testsDirPath;

	static {
		testsDirPath = TestUtil.getTestsDir();
	}
	
	private static final String fileWriteWithDelayDirName = "file-write-with-delay";
	private final static File fileWriteWithDelayDir = new File(testsDirPath, fileWriteWithDelayDirName);

	private static String skClasspath;
	private static String javaBin;
	private static String server2;
	
	@BeforeClass
	public static void setUpBeforeClass() {
		setupAndCheckTestsDirectory(fileWriteWithDelayDir);

		skClasspath = Util.getEnvVariable("SK_CLASSPATH");
		javaBin     = Util.getEnvVariable("JAVA_BIN");
		server2     = Util.getServer2();
	}
	
	@Test
	public void testWrite() throws InterruptedException {
		String filename = "file1";
		String size = "15000000";
		String rateLimit = "1";
		String className = FileWriteWithDelay.class.getCanonicalName();
		String[] commands = ProcessExecutor.getSshCommands(server2, "cd " + fileWriteWithDelayDir.getAbsolutePath() + "; " + javaBin + " -cp " + skClasspath + " " + className + " " + filename + "2 " + size + " " + rateLimit);
		ProcessExecutor.runCmdNoWait(commands);
		File f = new File(fileWriteWithDelayDir, filename);
		FileWriteWithDelay.main(new String[]{f.getAbsolutePath(), size, rateLimit});
	}
	
	public static void main(String[] args) throws IOException {
		if (args.length == 1)
			testsDirPath = TestUtil.getTestsDir( args[0] );
		
		Util.println("Running tests in: " + testsDirPath);
		Util.runTests(FileWriteWithDelayTest.class);
	}
}
