package com.ms.silverking.fs.test;

import static org.junit.Assert.*;
import static com.ms.silverking.fs.TestUtil.setupAndCheckTestsDirectory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

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
	
	@Test(timeout=45_000)
	public void testWrite_GiveWriter1AHeadstartExpectFileToBeAllWriter2Data() throws InterruptedException {
		File f = new File(fileWriteWithDelayDir, "this_file_should_be_the_data_of_the_remote_delayed_writer_aka_server2");
		int size = 15_000_000;
		int rateLimit = 1;
		String className = FileWriteWithDelay.class.getCanonicalName();
		int delaySeconds = 3;
		// if you're going to run from cmdline and test, use abs path names and set SK_CLASSPATH=/abs/path/to/repo/ide/eclipse/build-classes:/abs/path/to/repo/lib/*
		// lib/* will work here, you don't have to list each jar one by one
		String[] writer2Commands = ProcessExecutor.getSshCommandWithRedirectOutputFile(server2, "date; sleep " + delaySeconds + "; " + javaBin + " -cp " + skClasspath + " " + className + " " + f.getAbsolutePath() + " " + size + " " + rateLimit + " false; date; > /tmp/fwwd_w2.out");	// fwwd_w2.out exists, but is empty.. check ssh.out
		ProcessExecutor.runCmdNoWait(writer2Commands);
		System.out.println( ProcessExecutor.runCmd("date") );
		FileWriteWithDelay.main(new String[]{f.getAbsolutePath(), size+"", rateLimit+"", "true"});	// writer1
		System.out.println( ProcessExecutor.runCmd("date") );
		Thread.sleep(5_000);	// wait/allow some time for writer2 to finish writing to the file
		
		// Strictly speaking, the result is undefined
		// Practically speaking, I think that we will get what writer2 wrote unless there is something going on to slow down writer1's writes (rare, but something like an overloaded server)
		byte writer2ByteValue = FileWriteWithDelay.buffer2byteValue;
		checkContentsEquals(f, size, writer2ByteValue);
	}
	
	private void checkContentsEquals(File f, int size, byte expected) {
		try {
			InputStream out = new FileInputStream(f);
	
			for (int i = 0; i < size; i++) {
				byte actual = (byte)out.read();
				assertEquals("byte: " + i, expected, actual);
			}
			out.close();
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}
	
	public static void main(String[] args) throws IOException {
		if (args.length == 1)
			testsDirPath = TestUtil.getTestsDir( args[0] );
		
		Util.println("Running tests in: " + testsDirPath);
		Util.runTests(FileWriteWithDelayTest.class);
	}
}
