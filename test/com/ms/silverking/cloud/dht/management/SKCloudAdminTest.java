package com.ms.silverking.cloud.dht.management;

import static com.ms.silverking.cloud.dht.management.SKCloudAdmin.launchInstancesCommand;
import static com.ms.silverking.cloud.dht.management.SKCloudAdmin.stopInstancesCommand;
import static com.ms.silverking.cloud.dht.management.SKCloudAdmin.terminateInstancesCommand;
import static com.ms.silverking.testing.AssertFunction.test_SetterExceptions;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ms.silverking.testing.Util.ExceptionChecker;

public class SKCloudAdminTest {
		
	@Test
	public void testConstructor_Exceptions() {
		Object[][] testCases = {
			{"command = li",     new ExceptionChecker() { @Override public void check() { new SKCloudAdmin("li", SKCloudAdminOptions.defaultNumInstances, null, null, false); } }, IllegalArgumentException.class},
			{"numInstances = 0", new ExceptionChecker() { @Override public void check() { new SKCloudAdmin(launchInstancesCommand, 0, null, null, false); } },                     IllegalArgumentException.class},
		};
		
		test_SetterExceptions(testCases);
	}
	
	@Test
	public void testLaunchCommand() {
		SKCloudAdmin cloudAdmin = new SKCloudAdmin(launchInstancesCommand, 1, null, null, true);
		assertEquals(cloudAdmin.getCommand(), launchInstancesCommand);
		assertEquals(1,    cloudAdmin.getNumInstances());
		assertEquals(null, cloudAdmin.getAmiId());
		assertEquals(null, cloudAdmin.getInstanceType());
		assertEquals(true, cloudAdmin.getIncludeMaster());

		cloudAdmin = new SKCloudAdmin(launchInstancesCommand, 100, null, null, true);
	}
	
	@Test
	public void testStopCommand() {
		SKCloudAdmin cloudAdmin = new SKCloudAdmin(stopInstancesCommand);
		assertEquals(cloudAdmin.getCommand(), stopInstancesCommand);
		assertEquals(-1,   cloudAdmin.getNumInstances());
		assertEquals(null, cloudAdmin.getAmiId());
		assertEquals(null, cloudAdmin.getInstanceType());
		assertEquals(true, cloudAdmin.getIncludeMaster());
	}
	
	@Test
	public void testTerminateCommand() {
		SKCloudAdmin cloudAdmin = new SKCloudAdmin(terminateInstancesCommand);
		assertEquals(cloudAdmin.getCommand(), terminateInstancesCommand);
		assertEquals(-1,   cloudAdmin.getNumInstances());
		assertEquals(null, cloudAdmin.getAmiId());
		assertEquals(null, cloudAdmin.getInstanceType());
		assertEquals(true, cloudAdmin.getIncludeMaster());
	}
}
