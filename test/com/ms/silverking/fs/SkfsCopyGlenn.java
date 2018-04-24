package com.ms.silverking.fs;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Date;
import java.text.SimpleDateFormat;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import com.google.caliper.internal.guava.io.Files;
import com.ms.silverking.io.FileUtil;

public class SkfsCopyGlenn {

	private static final String nl  = System.lineSeparator();
	private static final String sep = File.separator;
	
	private static String targetDirPath;
	
	private static final String testsDirName  = "unit-tests";
	private static String absTestsDir;

	private static final String parentDirName  = "parentDir";

	private static File targetDir;
	private static File testsDir;
	private static File parentDir;

	private final static String testFilesDirName = "testFiles";
	private final File testFilesDir = new File(getClass().getResource(testFilesDirName).getPath());

	
	private static boolean exists = false;
	
	@BeforeClass
	public static void setUpBeforeClass() {
		printName("setUpBeforeClass");
		checkExists(targetDir);
		checkIsDir(targetDir);

		printDirContents("before delete");
		deleteRecursive(testsDir);
		printDirContents("after delete");
		checkDirIsEmpty(testsDir);
		createAndCheckDir(testsDir);
		printDirContents("after create");
	}
	
	private static void printName(String name) {
		System.out.println("**** " + name);
	}
	
	public static void printDirContents(String header) {
		System.out.println("  === " + header);
		long millis = System.currentTimeMillis();
		Date d = new Date(millis);
		SimpleDateFormat df = new SimpleDateFormat("hh:mm:ss:SS MM/dd/yyyy");
		String result = df.format(millis);
		System.out.println(result);
		System.out.println( runCmd(new String[]{"/bin/sh", "-c", "ls -lR " + targetDirPath}) );
	}
	
	private static void checkExists(File f) {
		assertTrue(f.exists());
	}

	private static void checkIsDir(File f) {
		assertTrue(f.isDirectory());
	}
	
	private static void checkDirIsEmpty(File f) {
		assertNull(f.listFiles());
	}
	
	private static void createAndCheckDir(File f) {
		assertTrue(f.mkdir());
	}
	
	private void createAndCheckDir() {
		createAndCheckDir(parentDir);
	}
	
	private static void deleteRecursive(File dir) {
		if (dir.exists()) {
			try {
				Files.deleteRecursively(dir);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	@Test
	public void testCopy_DirectoryTwice() {
		printName("testCopy_DirectoryTwice");
		for (int i = 0; i < 5; i++) {
			System.out.println("Round " + i);
			printName("\ttestCopy_Directory");
			createAndCheckDir();
			copyDirectories(testFilesDir, parentDir);
			deleteRecursive(parentDir);
			
			printName("\ttestCopy_Directory");
			System.out.println("parentDir exists? " + parentDir.exists());
			createAndCheckDir();
			copyDirectories(testFilesDir, parentDir);
			checkChecksumDir(parentDir, "1617524310 67");
		}
	}

	@Test
	public void testCopy_Directory() {
		printName("testCopy_Directory");
		printDirContents("before check");
		exists = parentDir.exists();
		System.out.println("parentDir exists? " + parentDir.exists());
		createAndCheckDir();
		copyDirectories(testFilesDir, parentDir);
		//checkChecksumDir(parentDir, "1617524310 67");
//		checkMd5sumDir(  parentDir, "7b309302eed5b6d6d8da561e1e0826e5");
		assertFalse(parentDir.delete());	// equivalent to rmdir, and "rmdir of a non-empty directory should return an error"
		deleteRecursive(parentDir);
	}
	
	private void copyDirectories(File src, File dest) {
		if (src.isFile())
			throw new RuntimeException("src needs to a be a directory");
		for (File srcFile : src.listFiles()) {
			File destFile = new File(dest, srcFile.getName());
			if (srcFile.isDirectory()) {
				destFile.mkdir();
				copyDirectories(srcFile, destFile);
			}
			else {
				try {
					Files.copy(srcFile, destFile);
				} catch (IOException e) {
					throw new RuntimeException("couldn't copy " + srcFile + " to " + destFile, e);
				}
			}
		}
	}
	
	private void checkChecksumDir(File f, String expected) {
		String output = runSumCmd("cksum", f);
		checkChecksum(output, expected);
	}
	
	private void checkChecksum(String output, String expected) {
		String[] fields = output.split(" ");
		
		String actual = fields[0] + " " + fields[1];
		assertEquals(expected, actual);
	}
	
	private String runSumCmd(String cmd, File f) {
		String absPath = f.getAbsolutePath() + sep;
		System.out.println(absPath);
		System.out.println( runCmd(new String[]{"/bin/sh", "-c", "find " + absPath + " -type f -exec " + cmd + " {} \\;"}));
		System.out.println( runCmd(new String[]{"/bin/sh", "-c", "find " + absPath + " -type f -exec " + cmd + " {} \\; | sed s#"+absPath+"##"}));
		String out =        runCmd(new String[]{"/bin/sh", "-c", "find " + absPath + " -type f -exec " + cmd + " {} \\; | sed s#"+absPath+"## | sort "});
		System.out.println(out);
		return out;
	}
	
	// http://stackoverflow.com/questions/5711084/java-runtime-getruntime-getting-output-from-executing-a-command-line-program
	private static String runCmd(String[] commands) {
		String output = "";
		Runtime rt = Runtime.getRuntime();
		try {
			Process p = rt.exec(commands);

			BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String s = null;
			while ((s = stdInput.readLine()) != null) 
				output += s + nl;
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return output;
	}
	
	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Need an argument for target directory: i.e. /var/tmp/silverking_holstben/skfs/skfs_mnt/skfs");
			return;
		}
		
		targetDirPath = args[0];
		absTestsDir   = targetDirPath + sep + testsDirName;
		
		targetDir = new File(targetDirPath);
		testsDir  = new File(targetDirPath, testsDirName);
		
		parentDir = new File(absTestsDir, parentDirName);
		
		Result result = JUnitCore.runClasses(SkfsCopyGlenn.class);
		printSummary(result);
		System.out.println("exists: " + exists);
	}
	
	public static void printSummary(Result result) {
		if (!result.wasSuccessful())
			System.out.println("\n\nErrors:");
			
		for (Failure failure : result.getFailures()) {
//			System.out.println(failure.getMessage());
//			System.out.println(failure.getTestHeader());
//			System.out.println(failure.getDescription());
//			System.out.println(failure.getException());
			System.out.println(failure.getTestHeader());
			System.out.println(failure.getTrace());
//			System.out.println(failure.toString());
		}
		
		int run     = result.getRunCount();
		int failed  = result.getFailureCount();
		int ignored = result.getIgnoreCount();
		int passed  = run - failed;
		
		System.out.println("PASSED:  " + passed);
		System.out.println("FAILED:  " + failed);
		System.out.println("IGNORED: " + ignored);
		
		System.out.println("All passed?: " + String.valueOf( result.wasSuccessful() ).toUpperCase());
	}

}
