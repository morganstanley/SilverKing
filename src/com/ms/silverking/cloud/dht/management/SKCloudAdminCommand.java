package com.ms.silverking.cloud.dht.management;

import static com.ms.silverking.cloud.dht.management.SKAdminCommand.commandDelimiter;

import com.ms.silverking.cloud.dht.management.aws.Util;

import java.util.Arrays;

public enum SKCloudAdminCommand {
  LaunchInstances, StartInstances, StopInstances, TerminateInstances, StartSpark, StopSpark;

  static void checkCommands(SKCloudAdminCommand[] commands) {
    if (0 == commands.length)   // should never happen b/c it's required in SKCloudAdminOptions
      Util.throwIllegalArgumentException("command", "", "you need to pass in a command");

    for (SKCloudAdminCommand command : commands)
      checkCommand(command);
  }

  private static void checkCommand(SKCloudAdminCommand command) {
    SKCloudAdminCommand[] commands = SKCloudAdminCommand.values();
    if (!Arrays.asList(commands).contains(command))
      Util.throwIllegalArgumentException("command", command, "must be: " + commands);
  }

  static boolean notALaunchCommand(SKCloudAdminCommand[] commands) {
    for (SKCloudAdminCommand command : commands)
      if (command == LaunchInstances)
        return false;

    return true;
  }

  public static SKCloudAdminCommand[] parseCommands(String commandsString) {
    String[] commandsArray = commandsString.split(commandDelimiter);
    SKCloudAdminCommand[] commands = new SKCloudAdminCommand[commandsArray.length];
    for (int i = 0; i < commandsArray.length; i++)
      commands[i] = SKCloudAdminCommand.valueOf(commandsArray[i]);
    return commands;
  }
}
