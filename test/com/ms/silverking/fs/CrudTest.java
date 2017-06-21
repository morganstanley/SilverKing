package com.ms.silverking.fs;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.ms.silverking.io.FileUtil.copyDirectories;
import static com.ms.silverking.fs.TestUtil.*;
import static com.ms.silverking.process.ProcessExecutor.*;

import com.ms.silverking.testing.Util;
import com.ms.silverking.testing.annotations.SkfsSmall;

@SkfsSmall
public class CrudTest {

	private static String testsDirPath;
	
	static {
		testsDirPath = TestUtil.getTestsDir();
	}
	
	private static final String crudDirName    = "crud";
	private static final String crudDirPath    = testsDirPath + sep + crudDirName;
	
	private static final String parentDirName  = "parentDir";
	private static final String parentDirPath  = crudDirPath + sep + parentDirName;
	
	private static final String parentFileName = "parentFile.txt";
	private static final String parentFilePath = crudDirPath + sep + parentFileName;

	private final static File crudDir   = new File(testsDirPath, crudDirName);
	
	private final File parentDir        = new File(crudDirPath, parentDirName);
	private final File parentDirRename  = new File(crudDirPath, parentDirName+"Rename");
	
	private final File parentFile       = new File(crudDirPath, parentFileName);
	private final File parentFileRename = new File(crudDirPath, parentFileName+"Rename");

	private final static String testFilesDirName = "testFiles";

	private final File testFilesDir = Util.getFile(getClass(), testFilesDirName, "");
	private final File singleLine   = Util.getFile(getClass(), testFilesDirName, "singleLineFile.txt");
	private final File multipleLine = Util.getFile(getClass(), testFilesDirName, "multipleLineFile.txt");
	
	private final File copy1 = new File(parentDirPath, "copy1");
	private final File copy2 = new File(parentDirPath, "copy2");
	
	@BeforeClass
	public static void setUpBeforeClass() {
		setupAndCheckTestsDirectory(crudDir);
	}

	@AfterClass
	public static void tearDownAfterClass() {
//		deleteTestFolder();
	}

//	@Before
//	public void setUp() throws Exception {
//		createDir();
//		createFile();
//	}
//
//	@After
//	public void tearDown() throws Exception {
//	}
	
	private void createAndCheckDir() {
		TestUtil.createAndCheckDir(parentDir);
	}
	
	private void deleteAndCheckDir() {
		TestUtil.deleteAndCheck(parentDir);
	}
	
	private void createAndCheckFile() {
		TestUtil.createAndCheckFile(parentFile);
	}
	
	private void checkRead(String contents) {
		TestUtil.checkRead( parentFile, contents);
	}
	
	private void checkWrite(String contents) {
		TestUtil.checkWrite(parentFile, contents);
	}
	
	@Test	
	public void testCreateAndDelete_Directory() {
//		printName("testCreateAndDelete_Directory");
		createAndCheckDir();
		checkDir(true);
		
		deleteAndCheckDir();
		checkDir(false);
	}
	
	private void checkDir(boolean expected) {
		assertEquals(expected,      parentDir.exists());
		assertEquals(expected,      parentDir.isDirectory());
		assertEquals(crudDirPath,   parentDir.getParent());
		assertEquals(parentDirPath, parentDir.getPath());
		assertEquals(parentDirName, parentDir.getName());
		
		if (expected)
			checkDirIsEmpty(parentDir);
		else
			checkDirIsNull(parentDir);
	}
	
	private void checkDirIsEmpty(File dir) {
		assertEquals(0, dir.listFiles().length);
	}
	
	private void checkDirIsNull(File dir) {
		assertNull(dir.listFiles());
	}
	
	@Test
	public void testCreateAndDelete_File() {
//		printName("testCreateAndDelete_File");
		createAndCheckFile();
		checkFile(true);
		
		deleteAndCheckFile();
		checkFile(false);
	}
	
	private void checkFile(boolean expected) {
		assertEquals(expected,       parentFile.exists());
		assertEquals(expected,       parentFile.isFile());
		assertEquals(expected,       parentFile.canRead());
		assertEquals(expected,       parentFile.canWrite());
		assertEquals(0,              parentFile.length());
//		assertTrue(0,                parentFile.lastModified());
		assertEquals(crudDirPath,    parentFile.getParent());
		assertEquals(parentFilePath, parentFile.getPath());
		assertEquals(parentFileName, parentFile.getName());
	}
	
	private void deleteAndCheckFile() {
		TestUtil.deleteAndCheck(parentFile);
	}
	
	@Test
	public void testRead() {
//		printName("testRead");
		createAndCheckFile();
		checkReadIsEmpty(parentFile);
		deleteAndCheckFile();
	}
	
	@Test
	public void testWrite() {
//		printName("testWrite");
		createAndCheckFile();
		
		String contents = "aba cadaba";
		checkWrite(contents);
		checkRead( contents);

		contents = "this is a first line?!#!@#?!@?# !?#?!@$?!@$2-=49159-0 285"+nl+"this is a second newline";
		checkWrite(contents);
		checkRead( contents);

		deleteAndCheckFile();
	}

	@Test
	public void testRename_Directory() {
//		printName("testRename_Directory");
		createAndCheckDir();
		assertFalse(parentDir.renameTo(parentDirRename));	// currently operation not supported in SKFS, so assertingFalse
		deleteAndCheckDir();
	}
	
	@Test
	public void testRename_File() {
//		printName("testRename_File");
		renamePreChecks();
		checkRename(parentFile, parentFileRename);
		renamePostChecks(parentFileRename);
	}
	
	private void renamePreChecks() {
		checkDirIsEmpty(crudDir);
		checkDoesntExist(parentFile);
		checkDoesntExist(parentFileRename);
		
		createAndCheckFile();
	}
	
	private void renamePostChecks(File renamed) {
		deleteAndCheck(renamed);
		
		checkDoesntExist(parentFile);
		checkDoesntExist(parentFileRename);
	}
	
//	@Test
	public void testRenameLoop_File() {
		for (int i = 0; i < 400; i++) {
			testRename_File();
		}
	}
	
	@Test
	public void testDoubleRename_File() {
//		printName("testDoubleRename_File");
		renamePreChecks();
		checkRename(parentFile,       parentFileRename);
		checkRename(parentFileRename, parentFile);
		renamePostChecks(parentFile);
	}
	
//	@Test
	public void testDoubleRenameLoop_File() {
		for (int i = 0; i < 400; i++) {
			testDoubleRename_File();
		}
	}
	
	@Test
	public void testCopy_Directory() {
//		printName("testCopy_Directory");
//		printDirContents("before check", crudDir);
		createAndCheckDir();
		copyDirectories(testFilesDir, parentDir);
//		checkChecksumDir(parentDir, "1617524310 67");
//		checkMd5sumDir(  parentDir, "7b309302eed5b6d6d8da561e1e0826e5");
		assertFalse(parentDir.delete());	// equivalent to rmdir, and "rmdir of a non-empty directory should return an error"
		deleteRecursive(parentDir);
	}

	@Test
	public void testCopy_File() {
//		printName("testCopy_File");
		createAndCheckDir();
		checkCopyAndRead(singleLine,   copy1, "this is a test file with one line",                                                                     "3402096896 33", "ffbe0d65deeba5b3abe46b0369372336");
		checkCopyAndRead(multipleLine, copy2, "this is a test file with multiple lines"+nl+"and I'm spanning"+nl+"multiple lines"+nl+"great"+nl+"EOF",  "286419593 81", "efee80a1e02ee54237b3484ab1657d50");
		deleteAndCheckDir();
	}
	
	private void checkCopyAndRead(File from, File to, String expectedContents, String expectedChecksum, String expectedMd5sum) {
		checkCopy(from, to);
		checkEquals(from, to);
		TestUtil.checkRead(to, expectedContents);
		checkChecksum(to, expectedChecksum);
		checkMd5sum(  to, expectedMd5sum);
		deleteAndCheck(to);
	}
	
	public static void main(String[] args) {
		if (args.length == 1)
			testsDirPath = TestUtil.getTestsDir( args[0] );
		
		Util.println("Running tests in: " + testsDirPath);
		Util.runTests(CrudTest.class);
	}
}
