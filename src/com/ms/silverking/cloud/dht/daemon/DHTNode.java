package com.ms.silverking.cloud.dht.daemon;

import java.util.Timer;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.daemon.storage.StorageModule;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ConvergenceController2;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.BaseOperation;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.BaseRetrievalEntryState;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.cloud.dht.meta.DaemonStateZK;
import com.ms.silverking.cloud.dht.meta.MetaClient;
import com.ms.silverking.cloud.dht.meta.NodeInfoZK;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.net.async.AsyncGlobals;
import com.ms.silverking.net.async.OutgoingData;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.time.AbsMillisTimeSource;
import com.ms.silverking.time.TimerDrivenTimeSource;
import com.ms.silverking.util.SafeTimer;

/**
 * Daemon that implements primary DHT functionality. 
 *
 */
public class DHTNode {
    private final String         dhtName;
    private final NodeRingMaster2 ringMaster;
    private final MessageModule  msgModule;
    private final StorageModule  storage;
    private final MemoryManager  memoryManager;
    private final DaemonStateZK  daemonStateZK;
    private final NodeInfoZK	 nodeInfoZK;
    private boolean       running = true;
     
    // FUTURE - make port non-static
    // also possibly make it a per-node rather than per-DHT notion
    private static int    serverPort;
    
    public static int getServerPort() {
        return serverPort;
    }
    
    // FUTURE - Make meta data updates use triggers and raise this interval, or
    // eliminate the need for it
    private static final long   updateIntervalMillis = 10 * 1000; 
    private static final double connectionPrimingDelaySeconds = 25.0;
    private static final double connectionPrimingPerNodeDelaySeconds = 0.2;
    
    private static final int    recoveryInactiveNodeTimeoutSeconds = 60;
    private static final int    inactiveNodeTimeoutSeconds = 30;
    
    private static final Timer                  daemonStateTimer;
    private static final Timer                  storageModuleTimer;
    private static final Timer                  messageModuleTimer;
    private static final int					timerDrivenTimeSourceResolutionMS = 5;
    private static final String					timeSourceTimerName = "TimeSourceTimer";
    private static final AbsMillisTimeSource    absMillisTimeSource;
    
    static {
    	DHTConstants.isDaemon = true;
        AsyncGlobals.setVerbose(true);
        absMillisTimeSource = new TimerDrivenTimeSource(new SafeTimer(timeSourceTimerName), timerDrivenTimeSourceResolutionMS);
        daemonStateTimer = new SafeTimer();
        storageModuleTimer = new SafeTimer();
        messageModuleTimer = new SafeTimer();
        OutgoingData.setAbsMillisTimeSource(absMillisTimeSource);
        BaseRetrievalEntryState.setAbsMillisTimeSource(absMillisTimeSource);
        BaseOperation.setAbsMillisTimeSource(absMillisTimeSource);
        ConvergenceController2.setAbsMillisTimeSource(absMillisTimeSource);
    }
    
	public DHTNode(String dhtName, ZooKeeperConfig zkConfig, int inactiveNodeTimeoutSeconds, boolean disableReap, boolean leaveTrash) {
	    try {
	        IPAndPort  daemonIPAndPort;
	        //DHTRingCurTargetWatcher	dhtRingCurTargetWatcher;
	        MetaClient	mc;
	        DHTConfiguration	dhtConfig;
	        
            Log.warning("LogLevel: ", Log.getLevel());
	        this.dhtName = dhtName;
	        mc = new MetaClient(dhtName, zkConfig);
	        dhtConfig = mc.getDHTConfiguration();
            Log.warning("DHTConfiguration: ", dhtConfig);
            serverPort = dhtConfig.getPort();
            daemonIPAndPort = MessageGroupBase.createLocalIPAndPort(serverPort);
            ringMaster = new NodeRingMaster2(dhtName, zkConfig, daemonIPAndPort);
            //dmw.addListener(ringMaster);
            Log.warning("Using port: "+ serverPort);
            daemonStateZK = new DaemonStateZK(mc, daemonIPAndPort, daemonStateTimer);
            daemonStateZK.setState(DaemonState.INITIAL_MAP_WAIT);
	        ringMaster.initializeMap(dhtConfig);
	        
            if (!daemonStateZK.waitForQuorumState(ringMaster.getAllCurrentReplicaServers(), DaemonState.INITIAL_MAP_WAIT, 
                    inactiveNodeTimeoutSeconds)) {
                daemonStateZK.waitForQuorumState(ringMaster.getAllCurrentReplicaServers(), DaemonState.INITIAL_MAP_WAIT, 
                        inactiveNodeTimeoutSeconds);
            }
            daemonStateZK.setState(DaemonState.RECOVERY);
            daemonStateZK.waitForQuorumState(ringMaster.getAllCurrentReplicaServers(), DaemonState.RECOVERY, 
            		recoveryInactiveNodeTimeoutSeconds);
            nodeInfoZK = new NodeInfoZK(mc, daemonIPAndPort, daemonStateTimer);       
            storage = new StorageModule(ringMaster, dhtName, storageModuleTimer, zkConfig, nodeInfoZK);
	        msgModule = new MessageModule(ringMaster, storage, absMillisTimeSource, messageModuleTimer, serverPort, 
	                                      mc);
	        memoryManager = new MemoryManager();
            storage.addMemoryObservers(memoryManager.getJVMMonitor());
            daemonStateZK.setState(DaemonState.QUORUM_WAIT);
            daemonStateZK.waitForQuorumState(ringMaster.getAllCurrentReplicaServers(), DaemonState.QUORUM_WAIT, 
                                             inactiveNodeTimeoutSeconds);
            daemonStateZK.setState(DaemonState.ENABLING_COMMUNICATION);
            if (!disableReap) {
            	storage.initialReap(leaveTrash);
            }
            msgModule.enable();
            daemonStateZK.waitForQuorumState(ringMaster.getAllCurrentReplicaServers(), DaemonState.ENABLING_COMMUNICATION, 
                                             inactiveNodeTimeoutSeconds);
            daemonStateZK.setState(DaemonState.COMMUNICATION_ENABLED);
            daemonStateZK.waitForQuorumState(ringMaster.getAllCurrentReplicaServers(), DaemonState.COMMUNICATION_ENABLED, 
                    inactiveNodeTimeoutSeconds);
            daemonStateZK.setState(DaemonState.PRIMING);
	        msgModule.start();
	        cleanVM();
            daemonStateZK.setState(DaemonState.RUNNING);
	    } catch (Exception e) {
	        throw new RuntimeException(e);
	    }
	}
	
    private void cleanVM() {
    	Runtime.getRuntime().runFinalization();
    	System.gc();
	}

	public void run() {
        while (running) {
        	synchronized (this) {
	            try {
	                this.wait();
	            } catch (InterruptedException ie) {
	            }
        	}
        }
    }
    
	public void test() {
	    Log.warning("DHTNode.test() starting");
	    Log.warning(msgModule);
	    ThreadUtil.sleepSeconds(1.0 * 60.0 * 60.0);
        Log.warning("DHTNode.test() complete");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	    try {
	        DHTNode            dhtNode;
	        String             dhtName;
	        ZooKeeperConfig    zkConfig;
	        DHTNodeOptions     options;
            CmdLineParser       parser;

	        LWTPoolProvider.createDefaultWorkPools();
	        
            options = new DHTNodeOptions();
            parser = new CmdLineParser(options);
            try {
                parser.parseArgument(args);
                
                dhtName = options.dhtName;
                zkConfig = new ZooKeeperConfig(options.zkConfig);                
                dhtNode = new DHTNode(dhtName, zkConfig, options.inactiveNodeTimeoutSeconds, options.disableReap, options.leaveTrash);
                //Log.setLevelAll();
                Log.initAsyncLogging();
                dhtNode.run();
                Log.warning("DHTNode run() returned cleanly");
            } catch (CmdLineException cle) {
            	Log.logErrorWarning(cle);
                System.err.println(cle.getMessage());
                parser.printUsage(System.err);
                return;
            } catch (Exception e) {
            	Log.logErrorWarning(e);
                e.printStackTrace();
            } finally {
                Log.warning("DHTNode leaving main()");
            }
	    } catch (Exception e) {
	        Log.logErrorWarning(e);
	    }
	}
}
