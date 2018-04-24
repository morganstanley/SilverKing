package com.ms.silverking.cloud.gridconfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ms.silverking.testing.Util;

import static com.ms.silverking.cloud.gridconfig.GridConfiguration.*;

public class GridConfigurationTest {

	private static final String testFilesDirName = "testFiles";
	private static final String envSuffix = ".txt";
	private static final String gcBadName  = "gc_bad";
	private static final String gcGoodName = "gc_good";
	private final File gcBad  = Util.getFile(getClass(), testFilesDirName, gcBadName  + envSuffix);
	private final File gcGood = Util.getFile(getClass(), testFilesDirName, gcGoodName + envSuffix);
	
	private static GridConfiguration nullGridConfig;
	
	private static Map<String, String> gcBadMap;
	private static Map<String, String> gcGoodMap;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		nullGridConfig = new GridConfiguration(null, null);
		
		gcBadMap = new HashMap<>();
		gcBadMap.put("GC_SK_PORT",   "8889");
		
		gcGoodMap = new HashMap<>();
		gcGoodMap.put("GC_SK_NAME",   "sk");
		gcGoodMap.put("GC_SK_ZK_LOC", "host:9981");
		gcGoodMap.put("GC_SK_PORT",   "8889");
	}

	@After
	public void tearDown() throws Exception {
		System.clearProperty(defaultBaseProperty);
		System.clearProperty(envSuffixProperty);
//		Util.removeEnv(defaultBaseEnvVar);	// this actually has no affect... it seems that we can only modify properties during a test run
	}

	// make sure GC_DEFAULT_BASE is NOT set when running the main of this test, i.e. cmdline> "unset GC_DEFAULT_BASE" before running main from cmdline
	@Test
	public void testStaticInitalizer_NoDefaultBasePropertyOrEnvVar() {
//		Util.printName("testStaticInitalizer_NoDefaultBasePropertyOrEnvVar");
		init();
	}
	
//	@Test
//	public void testStaticInitalizer_DefaultBasePropertyIsNull() {
//		setBasePropertyAndInit(null);	// won't work because map won't accept null values, so leaving this testcase commented out just for reference
//	}

//	@Test
//	public void testStaticInitalizer_DefaultBaseEnvVarIsNull() {
//		Util.setEnv(defaultBaseEnvVar, null);	// won't work because map won't accept null values, so leaving this testcase commented out just for reference
//		init();
//	}

	@Test
	public void testStaticInitalizer_DefaultBaseValLengthIsZero() {
//		Util.printName("testStaticInitalizer_DefaultBaseValLengthIsZero");
		setBasePropertyAndInit("");
	}
	
	@Test
	public void testStaticInitalizer_DefaultBaseValTrimLengthIsZero() {
//		Util.printName("testStaticInitalizer_DefaultBaseValTrimLengthIsZero");
		setBasePropertyAndInit("    ");
	}

	@Test
	public void testStaticInitalizer_DefaultBasePropertyNotNull() {
//		Util.printName("testStaticInitalizer_DefaultBasePropertyNotNull");
		setBasePropertyAndInit("a");
	}
	
	@Test
	public void testStaticInitalizer_EnvSuffixPropertyNotNull() {
//		Util.printName("testStaticInitalizer_EnvSuffixPropertyNotNull");
		setEnvSuffixProperty("a");
		init();
	}
	
	@Test(expected=RuntimeException.class)
	public void testGetDefaultBase_DefaultBaseValLengthIsZero() {
//		Util.printName("testGetDefaultBase_DefaultBaseValLengthIsZero");
		setBasePropertyAndInitAndGet("");
	}
	
	@Test(expected=RuntimeException.class)
	public void testGetDefaultBase_DefaultBaseValTrimLengthIsZero() {
//		Util.printName("testGetDefaultBase_DefaultBaseValTrimLengthIsZero");
		setBasePropertyAndInitAndGet("    ");
	}
	
	@Test
	public void testGetDefaultBase_DefaultBasePropertyNotNull() {
//		Util.printName("testGetDefaultBase_DefaultBasePropertyNotNull");
		setBasePropertyAndInitAndGet("a");
	}
	
	private static void setBasePropertyAndInit(String value) {
		setBaseProperty(value);
		init();
	}
	
	private static void setBasePropertyAndInitAndGet(String value) {
		setBaseProperty(value);
		initAndGetDefaultBase();
	}
	
	private static void setBaseProperty(String value) {
		setProperty(defaultBaseProperty, value);
	}
	
	private static void setEnvSuffixProperty(String value) {
		setProperty(envSuffixProperty, value);
	}
	
	private static void init() {
		GridConfiguration.staticInit();
	}
	
	private static void initAndGetDefaultBase() {
		init();
		GridConfiguration.getDefaultBase();
	}
	
	private static void setProperty(String key, String value) {
//		System.out.println("setting: " + key + " to " + value );
		System.setProperty(key, value);
//		System.out.println("set");
	}
	
	@Test(expected=NullPointerException.class)
	public void testNullObj_toEnvString() {
//		Util.printName("testNullObj_toEnvString");
		nullGridConfig.toEnvString();
	}

	@Test(expected=NullPointerException.class)
	public void testNullObj_get() {
//		Util.printName("testNullObj_get");
		nullGridConfig.get("");
	}

	@Test
	public void testCtors_Actual() {
//		Util.printName("testCtors_Actual");
		assertNull(nullGridConfig.getName());	
		assertNull(nullGridConfig.getEnvMap());
		assertEquals("null", nullGridConfig.toString());
	}

	@Test
	public void testParseFile() {
//		Util.printName("testParseFile");
		setEnvSuffixProperty(envSuffix);
		init();
		checkParseFile(gcBad.getParentFile(),  gcBadName,  gcBadMap);
		checkParseFile(gcGood.getParentFile(), gcGoodName, gcGoodMap);
	}
	
	private void checkParseFile(File gcBase, String gcName, Map<String, String> expectedMap) {
		try {
			assertEquals(new GridConfiguration(gcName, expectedMap), GridConfiguration.parseFile(gcBase, gcName));
		}
		catch (IOException e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testReadEnvFile() {
//		Util.printName("testReadEnvFile");
		checkReadEnvFile(gcBad,  gcBadMap);
		checkReadEnvFile(gcGood, gcGoodMap);
	}
	
	private void checkReadEnvFile(File f, Map<String, String> expectedMap) {
		try {
			assertEquals(expectedMap, GridConfiguration.readEnvFile(f));
		}
		catch (IOException e) {
			fail(e.getMessage());
		}
	}
	
	public static void main(String[] args) {
		Util.runTests(GridConfigurationTest.class);
	}
}
