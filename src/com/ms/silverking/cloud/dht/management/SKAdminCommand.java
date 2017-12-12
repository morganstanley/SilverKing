package com.ms.silverking.cloud.dht.management;

public enum SKAdminCommand {
	StartNodes, StopNodes, ClearData, LockInstance, UnlockInstance, CheckSKFS, StartSKFS, StopSKFS, CreateSKFSns, 
	ClearInstanceExclusions, SetInstanceExclusions, GetInstanceExclusions,
	ClearInstanceExclusionsData,
	GetActiveDaemons, EnsureNoActiveDaemons;
	
	public static final String	commandDelimiter = ",";
	
	public boolean isClusterCommand() {
		switch (this) {
		case StartNodes:
		case StopNodes:
		case ClearData:
		case StartSKFS:
		case CheckSKFS:
		case StopSKFS:
		case ClearInstanceExclusionsData:
			return true;
		case LockInstance:
		case UnlockInstance:
		case CreateSKFSns:
		case ClearInstanceExclusions:
		case SetInstanceExclusions:
		case GetInstanceExclusions:
		case GetActiveDaemons:
		case EnsureNoActiveDaemons:
			return false;
		default: throw new RuntimeException("panic");
		}
	}
	
	public static SKAdminCommand[] parseCommands(String s) {
		SKAdminCommand[]	commands;
		String[]			defs;
		
		defs = s.split(commandDelimiter);
		commands = new SKAdminCommand[defs.length];
		for (int i = 0; i < defs.length; i++) {
			commands[i] = SKAdminCommand.valueOf(defs[i]);
		}
		return commands;
	}
}
