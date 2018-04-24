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
import com.ms.silverking.thread.ThreadUtil;

public class SkfsRenameGlenn {

	private static final String nl  = System.lineSeparator();
	private static final String sep = File.separator;
	
	private static String targetDirPath;
	
	private static final String testsDirName  = "unit-tests";
	private static String absTestsDir;
	
	private static final String parentFileName = "parentFile.txt";

	private static File targetDir;
	private static File testsDir;
	
	private static File parentFile;
	private static File parentFileRename;

	
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
	
	private static void deleteRecursive(File dir) {
		if (dir.exists()) {
			try {
				Files.deleteRecursively(dir);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	private void createAndCheckFile() {
		try {
			assertTrue(parentFile.createNewFile());
		} catch (IOException e) {
			fail();
		}
	}
	
	private void deleteAndCheck(File f) {
		assertTrue(f.delete());
	}
	
	@Test
	public void test() throws InterruptedException {
		for (int i = 0; true; i++) {
			System.out.println("round " + i);
			rename();
		}
	}
	
	public void rename() {
		printName("rename modified");
		printDirContents("before createAndCheckFile");
		assertEquals(0, testsDir.listFiles().length);
		checkDoesntExist(parentFile);
		checkDoesntExist(parentFileRename);
		createAndCheckFile();
//		System.out.println("before checkRename");
		checkRename(parentFile, parentFileRename);
//		System.out.println("after checkRename");
		deleteAndCheck(parentFileRename);
		checkDoesntExist(parentFile);
		checkDoesntExist(parentFileRename);
		//ThreadUtil.sleepSeconds(1);
		ThreadUtil.sleepSeconds(0.1);
	}
	
	private void checkRename(File oldFile, File newFile) {
		checkDoesntExist(newFile);
		checkExists(oldFile);
		
		assertTrue(oldFile.renameTo(newFile));
		
		checkDoesntExist(oldFile);
		checkExists(newFile);
	}

	private void checkDoesntExist(File f) {
		assertFalse(f.exists());
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
			
			p.waitFor();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
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
		
		parentFile       = new File(absTestsDir, parentFileName);
		parentFileRename = new File(absTestsDir, parentFileName+"Rename");
		
		Result result = JUnitCore.runClasses(SkfsRenameGlenn.class);
		printSummary(result);
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
