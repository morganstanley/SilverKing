package com.ms.silverking.cloud.argus;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

import com.ms.silverking.log.Log;
import com.ms.silverking.text.StringUtil;

/**
 * Simple process terminator for Argus. When running in Armed mode, 
 * it will actually terminate processes. When running in LogOnly mode
 * it will only log the action that it would have taken.
 */
public class Terminator {
    private final static String killCmd = "/usr/bin/kill";
    
    private final Mode      mode;
    private final Runtime   runtime;
    private final TerminatorAsyncLogger logMessageHandler;
    private final KillType  killType;
    
    public enum Mode {LogOnly, Armed};
    public enum KillType {KillTerminator, CustomTerminator};
    
    private static Map<String,String>    killCommands;
    private static final String    PID_VARIABLE = "__PID__";
    
    static {
        killCommands = new HashMap<>();
        addKillCommand(KillType.KillTerminator.toString(), killCmd +" -9 "+ PID_VARIABLE);
    }
    
    public static void addKillCommand(String name, String cmd) {
        Log.warningf("addKillCommand '%s' '%s'", name, cmd);
        killCommands.put(name, cmd);
    }
    
    public Terminator(Mode mode, String loggerFileName, KillType killType) {
        this.mode = mode;
        runtime = Runtime.getRuntime();
        logMessageHandler = new TerminatorAsyncLogger(loggerFileName );
        this.killType = killType;
    }
    
    public Mode getMode() {
        return mode;
    }
    
    private String[] resolvedKillCommand(String name, int pid) {
        String    unresolvedCommand;
        
        unresolvedCommand = killCommands.get(name);
        if (unresolvedCommand == null) {
            Log.warning("No such kill command: "+ name);
            return null;
        } else {
            String    resolvedCommand;
            
            resolvedCommand = unresolvedCommand.replaceAll(PID_VARIABLE, Integer.toString(pid));
            return resolvedCommand.split("\\s+");
        }
    }
    
    public void terminate(int pid, String msg) {
        try {
            String[]    cmd;
            
            cmd = resolvedKillCommand(killType.toString(), pid);
            if (mode == Mode.LogOnly) {
                Log.warning(mode.name() + " Would have run: " + StringUtil.arrayToQuotedString(cmd) + " ", pid);
            } else {
                runtime.exec(cmd);
                Log.warning(mode.name() + " " + msg);
            }
            logMessageHandler.add(mode.name() + " " + msg);
        } catch (IOException ioe) {
            Log.logErrorWarning(ioe);
        }
    }
    
    public static void main(String[] args) {
        try {
            Terminator  terminator;
            KillType    killType;
            int         pid;
            
            killType = KillType.valueOf(args[0]);
            pid = Integer.parseInt(args[1]);
            terminator = new Terminator(Mode.Armed, "/tmp/test.terminator", killType);
            terminator.terminate(pid, "test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
