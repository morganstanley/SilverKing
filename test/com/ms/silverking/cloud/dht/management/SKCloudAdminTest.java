package com.ms.silverking.cloud.dht.management;

import static com.ms.silverking.cloud.dht.management.SKCloudAdmin.launchInstancesCommand;
import static com.ms.silverking.cloud.dht.management.SKCloudAdmin.startInstancesCommand;
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
			{"command = li",     new ExceptionChecker() { @Override public void check() { new SKCloudAdmin("li", SKCloudAdminOptions.defaultNumInstances, null, null, false, -1, null); } }, IllegalArgumentException.class},
			{"numInstances = 0", new ExceptionChecker() { @Override public void check() { new SKCloudAdmin(launchInstancesCommand, 0, null, null, false, -1, null); } },                     IllegalArgumentException.class},
			{"replication = 0",  new ExceptionChecker() { @Override public void check() { new SKCloudAdmin(launchInstancesCommand, 1, null, null, false, 0, null); } },                      IllegalArgumentException.class},
		};
		
		test_SetterExceptions(testCases);
	}
	
	@Test
	public void testLaunchCommand() {
		Object[][] testCases = {
			{1,       null,       null, true,  1, "/abc/123"},
			{100, "bf5ddd", "i3.metal", false, 4, "/var/tmp/silverking2"},
		};
		
		for (Object[] testCase : testCases) {
			int numInstances      =     (int)testCase[0];
			String amiId          =  (String)testCase[1];
			String instanceType   =  (String)testCase[2];
			boolean includeMaster = (boolean)testCase[3];
			int replication       =     (int)testCase[4];
			String dataBaseHome   =  (String)testCase[5];
			
			SKCloudAdmin cloudAdmin = new SKCloudAdmin(launchInstancesCommand, numInstances, amiId, instanceType, includeMaster, replication, dataBaseHome);
			assertEquals(cloudAdmin.getCommand(), launchInstancesCommand);
			checkVariables(cloudAdmin, numInstances, amiId, instanceType, includeMaster, replication, dataBaseHome);
		}
	}
	
	@Test
	public void testStartCommand() {
		checkCommand(startInstancesCommand);
	}
	
	@Test
	public void testStopCommand() {
		checkCommand(stopInstancesCommand);
	}
	
	@Test
	public void testTerminateCommand() {
		checkCommand(terminateInstancesCommand);
	}
	
	private void checkCommand(String command) {
		SKCloudAdmin cloudAdmin = new SKCloudAdmin(command);
		assertEquals(cloudAdmin.getCommand(), command);
		checkDefaults(cloudAdmin);
	}
	
	private void checkDefaults(SKCloudAdmin cloudAdmin) {
		checkVariables(cloudAdmin, -1, null, null, true, 1, "/var/tmp/silverking");
	}
	
	private void checkVariables(SKCloudAdmin cloudAdmin, int expectedNumInstances, String expectedAmiId, String expectedInstanceType, boolean expectedIncludeMaster, int expectedReplication, String expectedDataBaseHome) {
		assertEquals(expectedNumInstances,  cloudAdmin.getNumInstances());
		assertEquals(expectedAmiId,         cloudAdmin.getAmiId());
		assertEquals(expectedInstanceType,  cloudAdmin.getInstanceType());
		assertEquals(expectedIncludeMaster, cloudAdmin.getIncludeMaster());
		assertEquals(expectedReplication,   cloudAdmin.getReplication());
		assertEquals(expectedDataBaseHome,  cloudAdmin.getDataBaseHome());
	}
}
