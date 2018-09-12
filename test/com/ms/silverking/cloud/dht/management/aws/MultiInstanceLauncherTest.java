package com.ms.silverking.cloud.dht.management.aws;

import static com.ms.silverking.testing.AssertFunction.test_SetterExceptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.ms.silverking.testing.Util.ExceptionChecker;

public class MultiInstanceLauncherTest {

	private static final String ip = null;
	private static final List<String> nullList = nullList();
	
//	private MultiInstanceLauncher getNewTestLauncher() {
//		return new MultiInstanceLauncher(AmazonEC2ClientBuilder.defaultClient(), ip, 1, null, null, true);
//	}
	
	private MultiInstanceLauncher getNewNullLauncher(int numInstances, boolean includeMaster) {
		return new MultiInstanceLauncher(null, ip, numInstances, null, null, includeMaster);
	}
	
	private static List<String> nullList() {
		// can't do 'return Arrays.asList(null)' b/c you will get this: 
		//    java.lang.NullPointerException
		//    at java.util.Objects.requireNonNull(Objects.java:203)
		return Arrays.asList(ip);
	}

	@Test
	public void testConstructor_Exceptions() {
		Object[][] testCases = {
			{"numInstances = 0", new ExceptionChecker() { @Override public void check() { new MultiInstanceLauncher(null, ip, 0, null, null, true);	} }, IllegalArgumentException.class},
		};
		
		test_SetterExceptions(testCases);
	}
	
	// testRun and testCheckIamRoleIsAttached will only work if you run this test from an aws instance machine b/c if we try and create the AmazonEC2Client object we will get an exception, which results in these tests 
	// not really being run b/c the initial setup is killing the execution with the Exception, so we can't get our *actual* exceptions we are trying to test.
	// Actually it doesn't have to be from an aws instance machine, but the machine you run it from has to have the aws credentials, so either an IAMRole attached or the "config" and "credentials" files set.
	// If we get to that point though, and are on a credentialed machine, if we are running the tests from eclipse on that machine, we will need to edit the .classpath and add many more of the aws dependent jars to even get these tests running as well
//	@Test(expected = SdkClientException.class)
//	public void testRun() {
//		MultiInstanceLauncher launcher = getNewTestLauncher();
//		launcher.run();
//	}
//
//	@Test(expected = SdkClientException.class)
//	public void testCheckIamRoleIsAttached() {
//		MultiInstanceLauncher launcher = getNewTestLauncher();
//		launcher.checkIamRoleIsAttached();
//	}
	
	@Test
	public void testGetKeyPairName() {
		MultiInstanceLauncher launcher = getNewNullLauncher(1, true);
		assertEquals("sk_key_null", launcher.getKeyPairName());
	}
	
	@Test
	public void testGetInstanceIps() {
		MultiInstanceLauncher launcher = getNewNullLauncher(1, true);
		assertEquals(nullList, launcher.getInstanceIps());
	}
	
	@Test
	public void testGetWorkerIps() {
		MultiInstanceLauncher launcher = getNewNullLauncher(1, true);
		assertEquals(Collections.EMPTY_LIST, launcher.getWorkerIps());
	}
	
	@Test
	public void testMasterOnlyInstance() {
		MultiInstanceLauncher launcher = getNewNullLauncher(1, true);
		checkIsMasterOnly(launcher, true);
	}
	
	@Test
	public void testWorkerInstance() {
		MultiInstanceLauncher launcher = getNewNullLauncher(1, false);
		assertFalse(launcher.isMasterOnlyInstance());

		launcher = getNewNullLauncher(2, true);
		assertFalse(launcher.isMasterOnlyInstance());
	}
	
	private void checkIsMasterOnly(MultiInstanceLauncher launcher, boolean expected) {
		assertEquals(expected, launcher.isMasterOnlyInstance());
	}
}
