package com.ms.silverking.cloud.dht.management.aws;

import static com.ms.silverking.testing.AssertFunction.test_SetterExceptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.ms.silverking.testing.Util.ExceptionChecker;

public class MultiInstanceLauncherTest {

	private MultiInstanceLauncher getNewTestLauncher(int numInstances, boolean includeMaster) {
		return new MultiInstanceLauncher(ip, null, numInstances, null, null, includeMaster);
	}
	
	private static List<String> nullList() {
		// can't do 'return Arrays.asList(null)' b/c you will get this: 
		//    java.lang.NullPointerException
		//    at java.util.Objects.requireNonNull(Objects.java:203)
		return Arrays.asList(ip);
	}

	private static final String ip = null;
	private static final List<String> nullList = nullList();
	
	@Test
	public void testConstructor_Exceptions() {
		Object[][] testCases = {
			{"numInstances = 0", new ExceptionChecker() { @Override public void check() { new MultiInstanceLauncher(ip, null, 0, null, null, true);	} }, IllegalArgumentException.class},
		};
		
		test_SetterExceptions(testCases);
	}
	
	@Test
	public void testGetIpList() {
		MultiInstanceLauncher launcher = getNewTestLauncher(1, true);
		assertEquals(nullList(), launcher.getInstanceIps());
	}
	
	@Test
	public void testMasterOnlyInstance() {
		MultiInstanceLauncher launcher = getNewTestLauncher(1, true);
		assertTrue(launcher.isMasterOnlyInstance());
	}
	
	@Test
	public void testWorkerInstance() {
		MultiInstanceLauncher launcher = getNewTestLauncher(1, false);
		assertFalse(launcher.isMasterOnlyInstance());

		launcher = getNewTestLauncher(2, true);
		assertFalse(launcher.isMasterOnlyInstance());
	}
}
