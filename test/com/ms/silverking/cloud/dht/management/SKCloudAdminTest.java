package com.ms.silverking.cloud.dht.management;

import static com.ms.silverking.cloud.dht.management.SKCloudAdminCommand.LaunchInstances;
import static com.ms.silverking.cloud.dht.management.SKCloudAdminCommand.StartInstances;
import static com.ms.silverking.cloud.dht.management.SKCloudAdminCommand.StartSpark;
import static com.ms.silverking.cloud.dht.management.SKCloudAdminCommand.StopInstances;
import static com.ms.silverking.cloud.dht.management.SKCloudAdminCommand.StopSpark;
import static com.ms.silverking.cloud.dht.management.SKCloudAdminCommand.TerminateInstances;
import static com.ms.silverking.testing.AssertFunction.test_SetterExceptions;
import static com.ms.silverking.testing.Util.createToString;
import static com.ms.silverking.testing.Util.getTestMessage;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.ms.silverking.testing.Util.ExceptionChecker;
import org.junit.Test;

public class SKCloudAdminTest {

  @Test
  public void testConstructor_Exceptions() {
    Object[][] testCases = { { "command = null", new ExceptionChecker() {
      @Override
      public void check() { new SKCloudAdmin(null, null, 1, null, null, false, 1, null); }
    }, NullPointerException.class }, { "command = {}", new ExceptionChecker() {
      @Override
      public void check() { new SKCloudAdmin(getCommands(), null, 1, null, null, false, 1, null); }
    }, IllegalArgumentException.class }, { "command = {null}", new ExceptionChecker() {
      @Override
      public void check() { new SKCloudAdmin(getCommand(null), null, 1, null, null, false, 1, null); }
    }, IllegalArgumentException.class }, { "command = {LaunchInstancess, null}", new ExceptionChecker() {
      @Override
      public void check() { new SKCloudAdmin(getCommands(LaunchInstances, null), null, 1, null, null, false, 1, null); }
    }, IllegalArgumentException.class }, { "numInstances = 0", new ExceptionChecker() {
      @Override
      public void check() { new SKCloudAdmin(getCommand(LaunchInstances), null, 0, null, null, false, 1, null); }
    }, IllegalArgumentException.class }, { "replication = 0", new ExceptionChecker() {
      @Override
      public void check() { new SKCloudAdmin(getCommand(LaunchInstances), null, 1, null, null, false, 0, null); }
    }, IllegalArgumentException.class }, };

    test_SetterExceptions(testCases);
  }

  @Test
  public void testLaunchCommand() {
    Object[][] testCases = { { getCommand(LaunchInstances), "sk_key_myIp1.2.3", 1, null, null, true, 1, "/abc/123" },
        { getCommand(LaunchInstances), "sk_key_myIp_blah", 100, "bf5ddd", "i3.metal", false, 4,
            "/var/tmp/silverking2" }, { getCommands(LaunchInstances, StartSpark), "sk_key_myIp_1111", 100, "bf5ddd",
        "i3.metal", false, 4, "/var/tmp/silverking2" }, };

    for (Object[] testCase : testCases) {
      SKCloudAdminCommand[] commands = (SKCloudAdminCommand[]) testCase[0];
      String keyPairName = (String) testCase[1];
      int numInstances = (int) testCase[2];
      String amiId = (String) testCase[3];
      String instanceType = (String) testCase[4];
      boolean includeMaster = (boolean) testCase[5];
      int replication = (int) testCase[6];
      String dataBaseHome = (String) testCase[7];

      SKCloudAdmin cloudAdmin = new SKCloudAdmin(commands, keyPairName, numInstances, amiId, instanceType,
          includeMaster, replication, dataBaseHome);
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

  private void checkVariables(SKCloudAdmin cloudAdmin, int expectedNumInstances, String expectedAmiId,
      String expectedInstanceType, boolean expectedIncludeMaster, int expectedReplication,
      String expectedDataBaseHome) {
    assertEquals(expectedNumInstances, cloudAdmin.getNumInstances());
    assertEquals(expectedAmiId, cloudAdmin.getAmiId());
    assertEquals(expectedInstanceType, cloudAdmin.getInstanceType());
    assertEquals(expectedIncludeMaster, cloudAdmin.getIncludeMaster());
    assertEquals(expectedReplication, cloudAdmin.getReplication());
    assertEquals(expectedDataBaseHome, cloudAdmin.getDataBaseHome());
  }

  private SKCloudAdminCommand[] getCommand(SKCloudAdminCommand command) {
    return new SKCloudAdminCommand[] { command };
  }

  private SKCloudAdminCommand[] getCommands(SKCloudAdminCommand... commands) {
    return commands;
  }

  private void checkCommands(SKCloudAdmin cloudAdmin, SKCloudAdminCommand[] expectedCommands) {
    SKCloudAdminCommand[] actual = cloudAdmin.getCommands();
    assertArrayEquals(getTestMessage("checkCommands", "expected = " + createToString(expectedCommands),
        "actual   = " + createToString(actual)), expectedCommands, actual);
  }
}
