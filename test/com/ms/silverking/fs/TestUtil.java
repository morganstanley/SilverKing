package com.ms.silverking.fs;

import static com.ms.silverking.process.ProcessExecutor.runCmd;
import static com.ms.silverking.process.ProcessExecutor.sep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import com.google.caliper.internal.guava.io.Files;
import com.ms.silverking.io.FileUtil;
import com.ms.silverking.process.ProcessExecutor;
import com.ms.silverking.testing.Util;

public class TestUtil {

	static final String testsFolderName = "tests";

	public static String getTestsDir() {
		return getTestsDir( getDefaultSkfsRootPath() );
	}
	
	public static String getTestsDir(String dir) {
		return dir + sep + testsFolderName;
	}
	
	static String getDefaultSkfsRootPath() {
		String user = System.getProperty("user.name");
		String os   = System.getProperty("os.name").toLowerCase();
		
		String path;
		if (os.contains("windows")) {
			path = "C:\\Users\\" + user + "\\AppData\\Local\\Temp";
		}
		else if (os.equals("linux")) {
			String folderName = getSkFolderName();
			path = "/var/tmp/" + folderName + "/skfs/skfs_mnt/skfs";
		}
		else {
			path = "/var/tmp/silverking/data";
		}
		
		return path;
	}

	private static String getSkFolderName() {
		return Util.getEnvVariable("SK_FOLDER_NAME");
	}
	
	public static void setupAndCheckTestsDirectory(File testsDir) {
//		Util.printName("setupAndCheckTestsDirectory");
		File allTestsDir =    testsDir.getParentFile();
		File root        = allTestsDir.getParentFile();

		checkExists(root);
		checkIsDir(root);
		if ( !allTestsDir.exists() )
			createAndCheckDir(allTestsDir);
		
//		printDirContents("before delete");
		deleteRecursive(testsDir);
//		printDirContents("after delete");
		checkDoesntExist(testsDir);
		createAndCheckDir(testsDir);
//		printDirContents("after create");
	}

	static void printInfo(File f) {
		System.out.println("path Sep  : " + f.pathSeparator);
		System.out.println("path Sep c: " + f.pathSeparatorChar);
		System.out.println("sep       : " + f.separator);
		System.out.println("sep C     : " + f.separatorChar);
		System.out.println("abs f: " + f.getAbsoluteFile());
		System.out.println("abs d: " + f.getAbsolutePath());
		try {
			System.out.println("can f: " + f.getCanonicalFile());
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			System.out.println("can d: " + f.getCanonicalPath());
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(f.getName());
		System.out.println(f.getPath());
		System.out.println(f.getParent());
	}
	
	/////////////////////////////////////////////
	
	static void checkExists(File f) {
		assertTrue("" + f, f.exists());
	}

	static void checkDoesntExist(File f) {
		assertFalse("" + f, f.exists());
	}

	static void checkIsDir(File f) {
		assertTrue("" + f, f.isDirectory());
	}
	
	static void checkIsFile(File f) {
		assertTrue("" + f, f.isFile());
	}

	static void createAndCheckDir(File f) {
		assertTrue("" + f, f.mkdir());
	}
	
	static void deleteRecursive(File dir) {
		if (dir.exists()) {
			try {
				Files.deleteRecursively(dir);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	static void createAndCheckFile(File f) {
		try {
			assertTrue("" + f, f.createNewFile());
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}
	
	static void deleteAndCheck(File f) {
		assertTrue("" + f, f.delete());
	}

	static void checkRead(File f, String expected) {
		try {
			assertEquals(expected, FileUtil.readFileAsString(f));
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}
	
	static void checkReadIsEmpty(File f) {
		try {
			assertEquals(0, FileUtil.readFileAsBytes(f).length);
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}
	
	
	static void checkWrite(File f, String contents) {
		try {
			FileUtil.writeToFile(f, contents);
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}
	
	static void checkCopy(File from, File to) {
		try {
			Files.copy(from, to);
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}
	
	static void checkRename(File oldFile, File newFile) {
		checkDoesntExist(newFile);
		checkExists(oldFile);
		
		assertTrue(oldFile.renameTo(newFile));
		
		checkDoesntExist(oldFile);
		checkExists(newFile);
	}
	
	// since assertEquals(from, to); compares file names and not contents, creating my own...
	static void checkEquals(File from, File to) {
		checkExists(from);
		checkExists(to);
		
		checkIsFile(from);
		checkIsFile(to);
		
		assertEquals(from.length(), to.length());
	}
	
	static void checkChecksum(File f, String expected) {
		String output = runCmd("cksum", f);
		checkChecksum(output, expected);
	}
	
//	private void checkChecksumDir(File f, String expected) {
//		String output = runSumCmd("cksum", f);
//		checkChecksum(output, expected);
//	}
	
	private static void checkChecksum(String output, String expected) {
		String[] fields = output.split(" ");
		
		String actual = fields[0] + " " + fields[1];
		assertEquals(expected, actual);
	}
	
	static void checkMd5sum(File f, String expected) {
		String output = runCmd("md5sum", f);
		checkMd5sum(output, expected);
	}
	
//	private void checkMd5sumDir(File f, String expected) {
//		String output = runSumCmd("md5sum", f);
//		checkMd5sum(output, expected);
//	}
	
	private static void checkMd5sum(String output, String expected) {
		String[] fields = output.split(" ");
		
		String actual = fields[0];
		if (actual.startsWith("\\")) // windows weirdness
			actual = actual.substring(1);
		assertEquals(expected, actual);
	}
	
	public static void testExecutionWasGood(ProcessExecutor pe) {
		testExecuteGood(pe);
		testDidntTimeout(pe);
		testExitCodeGood(pe);
	}

	private static void testExecuteGood(ProcessExecutor pe) {
		try {
			pe.execute();
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}
	
	private static void testDidntTimeout(ProcessExecutor pe) {
		assertFalse(pe.timedOut());
	}
	
	private static void testExitCodeGood(ProcessExecutor pe) {
		int expected = 0;
		int actual = pe.getExitCode();
		assertEquals("expecting exit code == " + expected + ", was " + actual, expected, actual); 
	}
}
