package com.ms.silverking.cloud.dht.management.aws;

import static com.ms.silverking.cloud.dht.management.aws.MultiInstanceLauncher.newLine;
import static com.ms.silverking.testing.AssertFunction.test_SetterExceptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import com.ms.silverking.testing.Util.ExceptionChecker;

public class MultiInstanceLauncherTest {

	private MultiInstanceLauncher getNewTestLauncher(int numInstances, boolean includeMaster) {
		return new MultiInstanceLauncher(null, null, numInstances, null, null, includeMaster);
	}
	
	@Test
	public void testConstructor_Exceptions() {
		Object[][] testCases = {
			{"numInstances = 0", new ExceptionChecker() { @Override public void check() { new MultiInstanceLauncher(null, null, 0, null, null, true); } }, IllegalArgumentException.class},
		};
		
		test_SetterExceptions(testCases);
	}
	
	@Test
	public void testGetIpList() {
		MultiInstanceLauncher launcher = getNewTestLauncher(1, true);
		assertEquals("null"+newLine, launcher.getIpList());
	}
	
	@Test
	public void testMasterOnlyInstance() {
		MultiInstanceLauncher launcher = getNewTestLauncher(1, true);
		assertTrue(launcher.isMasterOnlyInstance());
	}
	
	@Test
	public void testMasterInstanceOnly() {
		MultiInstanceLauncher launcher = getNewTestLauncher(1, false);
		assertFalse(launcher.isMasterOnlyInstance());

		launcher = getNewTestLauncher(2, true);
		assertFalse(launcher.isMasterOnlyInstance());

		launcher = getNewTestLauncher(1, false);
		assertFalse(launcher.isMasterOnlyInstance());
	}
}
