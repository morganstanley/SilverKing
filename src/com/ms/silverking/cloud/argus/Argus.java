package com.ms.silverking.cloud.argus;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.ms.silverking.log.Log;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.LogMode;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;
import com.ms.silverking.util.SafeTimer;

/** * Argus ensures that servers are not damaged by user processes * by enforcing a set of safety constraints. These constraints * are enforced by concrete SafetyEnforcer implementations.  */
public class Argus {    private static final String propKillEnabled = "killEnabled";     private static final String defaultKillEnabled = "false";    private static final String propEventsLogDir = "eventsLogDir";     private static final String defaultEventsLogDir = "/tmp";    private final List<SafetyEnforcer>  enforcers;    private final Timer                 timer;    private enum Test {RSS, DiskUsage};    private boolean killEnabled ;    private Terminator terminator;    private Terminator.KillType killtype;        public Argus(ArgusOptions options) throws IOException {        enforcers = new ArrayList<>();        killtype = Terminator.KillType.valueOf( options.terminatorType );
        if (options.DiskUsageEnforcer != null) {        	enforcers.add(initEnforcer(Test.DiskUsage, options.DiskUsageEnforcer, options));
        }        enforcers.add(initEnforcer(Test.RSS, options.RSSEnforcer, options));    	        timer = new SafeTimer();    }    private SafetyEnforcer initEnforcer(Test test, String testSpec, ArgusOptions options) throws IOException {        Properties prop = new Properties();        try {            FileInputStream fis = new FileInputStream(testSpec);            prop.load(fis);            fis.close();        } catch (Exception e) {            Log.warning(e);        }        PropertiesHelper	ph;
        
        ph = new PropertiesHelper(prop, LogMode.UndefinedAndExceptions);                if (terminator == null) {            String	eventsLogDir;
            String	customTerminatorDef;
            
            eventsLogDir = ph.getString(propEventsLogDir, defaultEventsLogDir);            if (eventsLogDir == null){                throw new RuntimeException(propEventsLogDir + " is not specified in Properties file "+ testSpec);            }            String loggerFileName = eventsLogDir + "/" + InetAddress.getLocalHost().getHostName();            killEnabled = Boolean.parseBoolean(ph.getString(propKillEnabled, defaultKillEnabled));            terminator = new Terminator(killEnabled ? Terminator.Mode.Armed : Terminator.Mode.LogOnly, loggerFileName, killtype);            Log.warning("Argus terminator is running with mode " + terminator.getMode().name() + " and termination type: ", killtype);
            
            customTerminatorDef = ph.getString(Terminator.KillType.CustomTerminator.toString(), UndefinedAction.ZeroOnUndefined);
            if (customTerminatorDef != null) {
            	Terminator.addKillCommand(Terminator.KillType.CustomTerminator.toString(), customTerminatorDef);
            }
        }        
        switch (test) {        case DiskUsage: return new DiskUsageEnforcer(ph, terminator);        case RSS: 
            Log.warning("RSSCandidateComparisonMode: ", options.rssCandidateComparisonMode);
            return new RSSEnforcer(ph, terminator, options);        default: throw new RuntimeException("Unimplemented test: "+ testSpec);        }    }
    public void enforce() {        for (SafetyEnforcer enforcer : enforcers) {            timer.schedule(new ArgusTask(enforcer), 0);        }    }
    /**     * @param args cmd-line args     */    public static void main(String[] args) {        try {            //Log.setLevelAll();            //Log.setPrintStreams(System.out);        	
            Log.warning("Argus is starting");
                    	ArgusOptions   options = new ArgusOptions();        	CmdLineParser  parser  = new CmdLineParser(options);    		try {    			parser.parseArgument(args);    		} catch (CmdLineException cle) {    			System.err.println(cle.getMessage());    			parser.printUsage(System.err);    			return;    		}    		new Argus(options).enforce();        	    		/*            if (args.length == 0) {                System.out.println("args: <RSS:propertyFile> <DiskUsage:propertyFile> ");                return;            } else {                List<String>   testSpecs;                                testSpecs = Arrays.asList(args);                new Argus(testSpecs).enforce();            }            */        } catch (Exception e) {            Log.logErrorWarning(e);        }    }
    private class ArgusTask extends TimerTask {        private final SafetyEnforcer    enforcer;
        ArgusTask(SafetyEnforcer enforcer) {            this.enforcer = enforcer;        }
        @Override        public void run() {            int delayMillis;
            delayMillis = enforcer.enforce();            timer.schedule(new ArgusTask(enforcer), delayMillis);        }    }}
