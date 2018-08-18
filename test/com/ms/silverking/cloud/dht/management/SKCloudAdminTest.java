package com.ms.silverking.cloud.dht.management;

import static com.ms.silverking.cloud.dht.management.SKCloudAdminCommand.LaunchInstances;
import static com.ms.silverking.cloud.dht.management.SKCloudAdminCommand.StartInstances;
import static com.ms.silverking.cloud.dht.management.SKCloudAdminCommand.StartSpark;
import static com.ms.silverking.cloud.dht.management.SKCloudAdminCommand.StopInstances;
import static com.ms.silverking.cloud.dht.management.SKCloudAdminCommand.StopSpark;
import static com.ms.silverking.cloud.dht.management.SKCloudAdminCommand.TerminateInstances;
import static com.ms.silverking.testing.AssertFunction.test_SetterExceptions;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ms.silverking.testing.Util.ExceptionChecker;
import static com.ms.silverking.testing.Util.createToString;
import static com.ms.silverking.testing.Util.getTestMessage;


import static org.junit.Assert.assertArrayEquals;

public class SKCloudAdminTest {
		
    // can't go in testConstructor_Exceptions b/c the null won't get past, ec.check();, in com.ms.silverking.testing.Assert.exceptionNameChecker - since ec = null
    @Test(expected = NullPointerException.class)
    public void testConstructor_NullException() {
		new SKCloudAdmin(null, SKCloudAdminOptions.defaultNumInstances, null, null, false, -1, null);
    }
    
	@Test
	public void testConstructor_Exceptions() {
		Object[][] testCases = {
			{"command = {}",                       new ExceptionChecker() { @Override public void check() { new SKCloudAdmin(getCommands(),                      1, null, null, false, 1, null); } }, IllegalArgumentException.class},
			{"command = {null}",                   new ExceptionChecker() { @Override public void check() { new SKCloudAdmin(getCommand(null),                   1, null, null, false, 1, null); } }, IllegalArgumentException.class},
			{"command = {LaunchInstancess, null}", new ExceptionChecker() { @Override public void check() { new SKCloudAdmin(getCommands(LaunchInstances, null), 1, null, null, false, 1, null); } }, IllegalArgumentException.class},
			{"numInstances = 0",                   new ExceptionChecker() { @Override public void check() { new SKCloudAdmin(getCommand(LaunchInstances),        0, null, null, false, 1, null); } }, IllegalArgumentException.class},
			{"replication = 0",                    new ExceptionChecker() { @Override public void check() { new SKCloudAdmin(getCommand(LaunchInstances),        1, null, null, false, 0, null); } }, IllegalArgumentException.class},
		};
		
		test_SetterExceptions(testCases);
	}
	
	@Test
	public void testLaunchCommand() {
		Object[][] testCases = {
			{getCommand(LaunchInstances),                1,     null,       null, true,  1, "/abc/123"},
			{getCommand(LaunchInstances),              100, "bf5ddd", "i3.metal", false, 4, "/var/tmp/silverking2"},
			{getCommands(LaunchInstances, StartSpark), 100, "bf5ddd", "i3.metal", false, 4, "/var/tmp/silverking2"},
		};
		
		for (Object[] testCase : testCases) {
			SKCloudAdminCommand[] commands = (SKCloudAdminCommand[])testCase[0];
			int numInstances               =                   (int)testCase[1];
			String amiId                   =                (String)testCase[2];
			String instanceType            =                (String)testCase[3];
			boolean includeMaster          =               (boolean)testCase[4];
			int replication                =                   (int)testCase[5];
			String dataBaseHome            =                (String)testCase[6];
			
			SKCloudAdmin cloudAdmin = new SKCloudAdmin(commands, numInstances, amiId, instanceType, includeMaster, replication, dataBaseHome);
			checkCommands(cloudAdmin, commands);
			checkVariables(cloudAdmin, numInstances, amiId, instanceType, includeMaster, replication, dataBaseHome);
		}
	}
	
	@Test
	public void testStartCommand() {
		checkCommand(StartInstances);
	}
	
	@Test
	public void testStopCommand() {
		checkCommand(StopInstances);
	}
	
	@Test
	public void testTerminateCommand() {
		checkCommand(TerminateInstances);
	}
	
	@Test
	public void testStartSparkCommand() {
		checkCommand(StartSpark);
	}
	
	@Test
	public void testStopSparkCommand() {
		checkCommand(StopSpark);
	}
	
	private void checkCommand(SKCloudAdminCommand command) {
		SKCloudAdmin cloudAdmin = new SKCloudAdmin(command);
		checkCommandName(cloudAdmin, command);
		checkDefaults(cloudAdmin);
	}
	
	private void checkCommandName(SKCloudAdmin cloudAdmin, SKCloudAdminCommand command) {
		assertEquals(command, cloudAdmin.getCommands()[0]);
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
    
    private SKCloudAdminCommand[] getCommand(SKCloudAdminCommand command) {
        return new SKCloudAdminCommand[]{command};
    }
    
    private SKCloudAdminCommand[] getCommands(SKCloudAdminCommand... commands) {
        return commands;
    }
    
    private void checkCommands(SKCloudAdmin cloudAdmin, SKCloudAdminCommand[] expectedCommands) {
        SKCloudAdminCommand[] actual = cloudAdmin.getCommands();
        assertArrayEquals(getTestMessage("checkCommands",
				"expected = " + createToString(expectedCommands),
				"actual   = " + createToString(actual)), 
                expectedCommands, actual);
    }
}
