package com.ms.silverking.cloud.dht.daemon;

import java.net.InetAddress;
import java.util.Timer;

import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.JVMUtil;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.storage.ReapPolicy;
import com.ms.silverking.cloud.dht.daemon.storage.StorageModule;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ConvergenceController2;
import com.ms.silverking.cloud.dht.daemon.storage.management.ManagedStorageModule;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.BaseOperation;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.BaseRetrievalEntryState;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.cloud.dht.meta.DaemonStateZK;
import com.ms.silverking.cloud.dht.meta.IpAliasConfiguration;
import com.ms.silverking.cloud.dht.meta.MetaClient;
import com.ms.silverking.cloud.dht.meta.NodeInfoZK;
import com.ms.silverking.cloud.dht.net.ExclusionSetAddressStatusProvider;
import com.ms.silverking.cloud.dht.net.IPAliasMap;
import com.ms.silverking.cloud.dht.net.IPAliasingUtil;
import com.ms.silverking.cloud.dht.trace.TracerFactory;
import com.ms.silverking.cloud.toporing.ResolvedReplicaMap;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.net.async.AsyncGlobals;
import com.ms.silverking.net.async.OutgoingData;
import com.ms.silverking.process.LogAndExitUncaughtExceptionHandler;
import com.ms.silverking.process.SafeThread;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.time.AbsMillisTimeSource;
import com.ms.silverking.util.SafeTimer;

/**
 * Daemon that implements primary DHT functionality.
 */
public class DHTNode {
  private final String dhtName;
  private final NodeRingMaster2 ringMaster;
  private final MessageModule msgModule;
  private final StorageModule storage;
  private final MemoryManager memoryManager;
  private final DaemonStateZK daemonStateZK;
  private final NodeInfoZK nodeInfoZK;
  private final MetaClient mc;
  private final boolean enableMsgGroupTrace;

  private boolean running;

  // FUTURE - make port non-static
  // also possibly make it a per-node rather than per-DHT notion
  private static volatile int actualPort = DHTConstants.uninitializedPort;
  private static volatile int dhtPort = DHTConstants.uninitializedPort;

  // this is the port fixed in DHT config, though the server may really be listening on another port
  // this port forms the identity of the node. For a node to run on a different port to this,
  // alias mapping must be used - See MessageModuleBase for a comment describing this feature
  public static int getDhtPort() {
    return dhtPort;
  }

  // The port the server is listening on, which may != dhtPort, if overridden in constructor
  public static int getActualPort() {
    return actualPort;
  }

  // FUTURE - Make meta data updates use triggers and raise this interval, or
  // eliminate the need for it
  private static final long updateIntervalMillis = 10 * 1000;
  private static final double connectionPrimingDelaySeconds = 25.0;
  private static final double connectionPrimingPerNodeDelaySeconds = 0.2;

  private static final int recoveryInactiveNodeTimeoutSeconds = 60;
  private static final int inactiveNodeTimeoutSeconds = 30;

  private static final Timer daemonStateTimer;
  private static final Timer storageModuleTimer;
  private static final Timer messageModuleTimer;
  private static final AbsMillisTimeSource absMillisTimeSource;

  static {
    DHTConstants.isDaemon = true;
    AsyncGlobals.setVerbose(true);
    absMillisTimeSource = SystemTimeUtil.timerDrivenTimeSource;
    daemonStateTimer = new SafeTimer();
    storageModuleTimer = new SafeTimer();
    messageModuleTimer = new SafeTimer();
    OutgoingData.setAbsMillisTimeSource(absMillisTimeSource);
    BaseRetrievalEntryState.setAbsMillisTimeSource(absMillisTimeSource);
    BaseOperation.setAbsMillisTimeSource(absMillisTimeSource);
    ConvergenceController2.setAbsMillisTimeSource(absMillisTimeSource);
    SafeThread.setDefaultUncaughtExceptionHandler(new LogAndExitUncaughtExceptionHandler());
  }

  public DHTNode(String dhtName, ZooKeeperConfig zkConfig, DHTNodeConfiguration nodeConfig,
      int inactiveNodeTimeoutSeconds, ReapPolicy reapPolicy, int overridePort, String daemonIP) {
    try {
      running = true;
      //DHTRingCurTargetWatcher    dhtRingCurTargetWatcher;
      DHTConfiguration dhtConfig;
      IpAliasConfiguration aliasConfig;
      ExclusionSetAddressStatusProvider exclusionSetAddressStatusProvider;
      IPAliasMap aliasMap;
      IPAndPort daemonIPAndPort;
      IPAndPort baseInterfaceIPAndPort;

      Log.warning("LogLevel: ", Log.getLevel());
      this.dhtName = dhtName;
      mc = new MetaClient(dhtName, zkConfig);
      dhtConfig = mc.getDHTConfiguration();
      Log.warning("DHTConfiguration: ", dhtConfig);
      aliasConfig = mc.getIpAliasConfiguration(dhtConfig.getIpAliasMapName());
      aliasMap = IPAliasingUtil.readAliases(dhtConfig, aliasConfig);

      dhtPort = dhtConfig.getPort();
      ResolvedReplicaMap.setDHTPort(dhtConfig.getPort());

      // Determine daemonIPAndPort and interface ip and port
      daemonIPAndPort = null;
      baseInterfaceIPAndPort = null;
      if (daemonIP != null && overridePort != DHTConstants.noPortOverride) {
        throw new RuntimeException("Only one of daemonIP and overridePort may be set");
      }
      if (daemonIP != null) { // daemon ip specified
        Log.info("daemonIP specified");
        daemonIPAndPort = new IPAndPort(daemonIP, dhtConfig.getPort());
        baseInterfaceIPAndPort = (IPAndPort)aliasMap.daemonToInterface(daemonIPAndPort);
        if (baseInterfaceIPAndPort == null) {
          baseInterfaceIPAndPort = daemonIPAndPort;
        }
      } else if (overridePort != DHTConstants.noPortOverride) { // override port specified
        Log.info("overridePort specified");
        baseInterfaceIPAndPort = new IPAndPort(InetAddress.getLocalHost().getHostAddress(), overridePort);
        daemonIPAndPort = aliasMap.interfaceToDaemon(baseInterfaceIPAndPort);
        if (daemonIPAndPort == null) {
          throw new RuntimeException("Alias map has no entry for interface: "+ baseInterfaceIPAndPort);
        }
      } else { // neither daemon ip nor override port specified
        Log.info("Neither daemonIP nor overridePort specified");
        // Check if the local interface ip happens to uniquely identify a daemon
        daemonIPAndPort = aliasMap.interfaceIPToDaemon_ifUnique(InetAddress.getLocalHost().getHostAddress());
        if (daemonIPAndPort != null) {
          Log.infof("Found unique alias for ip %s", InetAddress.getLocalHost().getHostAddress());
          baseInterfaceIPAndPort = (IPAndPort)aliasMap.daemonToInterface(daemonIPAndPort);
        } else {
          // use default ip : dht port for both daemon and interface
          Log.infof("No unique alias found for ip; setting daemonIP to interfaceIP %s",
              InetAddress.getLocalHost().getHostAddress());
          baseInterfaceIPAndPort = new IPAndPort(InetAddress.getLocalHost().getHostAddress(), dhtConfig.getPort());
          daemonIPAndPort = baseInterfaceIPAndPort;
        }
      }
      Log.infof("daemonIPAndPort: %s", daemonIPAndPort);
      Log.infof("baseInterfaceIPAndPort: %s", baseInterfaceIPAndPort);
      if (!IPAndPort.equalIPs(daemonIPAndPort, baseInterfaceIPAndPort)) {
        IPAddrUtil.ensureLocalIP(daemonIPAndPort.getIPAsString());
      }
      IPAndPort configuredLocalPort = new IPAndPort(IPAddrUtil.localIP(), dhtPort);
      if (!daemonIPAndPort.equals(configuredLocalPort)) {
        throw new RuntimeException("Daemon ip and port " + daemonIPAndPort.toString() + " did not match local configured address " + configuredLocalPort.toString());
      }
      actualPort = baseInterfaceIPAndPort.getPort();

      this.enableMsgGroupTrace = dhtConfig.getEnableMsgGroupTrace();
      Log.warning("EnableMsgGroupTrace: ", enableMsgGroupTrace);
      if (enableMsgGroupTrace) {
        TracerFactory.ensureTracerInitialized();
      }

      exclusionSetAddressStatusProvider = new ExclusionSetAddressStatusProvider(MessageModule.nodePingerThreadName, aliasMap);
      ringMaster = new NodeRingMaster2(dhtName, zkConfig, daemonIPAndPort);
      ringMaster.setExclusionSetAddressStatusProvider(exclusionSetAddressStatusProvider);
      //dmw.addListener(ringMaster);
      Log.warning("ReapPolicy: ", reapPolicy);
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
      nodeInfoZK = new NodeInfoZK(mc, nodeConfig, daemonIPAndPort, daemonStateTimer);
      memoryManager = new MemoryManager();
      storage = new StorageModule(ringMaster, dhtName, storageModuleTimer, zkConfig, nodeInfoZK, reapPolicy,
          memoryManager.getJVMMonitor(), enableMsgGroupTrace);
      msgModule = new MessageModule(ringMaster, storage, absMillisTimeSource, messageModuleTimer,
          baseInterfaceIPAndPort.getPort(), daemonIPAndPort, mc,aliasMap, enableMsgGroupTrace);
      msgModule.setAddressStatusProvider(exclusionSetAddressStatusProvider);
      daemonStateZK.setState(DaemonState.QUORUM_WAIT);
      daemonStateZK.waitForQuorumState(ringMaster.getAllCurrentReplicaServers(), DaemonState.QUORUM_WAIT,
          inactiveNodeTimeoutSeconds);
      daemonStateZK.setState(DaemonState.ENABLING_COMMUNICATION);
      msgModule.enable();
      daemonStateZK.waitForQuorumState(ringMaster.getAllCurrentReplicaServers(), DaemonState.ENABLING_COMMUNICATION,
          inactiveNodeTimeoutSeconds);
      daemonStateZK.setState(DaemonState.COMMUNICATION_ENABLED);
      daemonStateZK.waitForQuorumState(ringMaster.getAllCurrentReplicaServers(), DaemonState.COMMUNICATION_ENABLED,
          inactiveNodeTimeoutSeconds);
      daemonStateZK.setState(DaemonState.PRIMING);
      msgModule.start();
      cleanVM();
      daemonStateZK.setState(DaemonState.INITIAL_REAP);
      storage.startupReap();
      storage.setReady();
      daemonStateZK.setState(DaemonState.RUNNING);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public DHTNode(DHTNodeOptions options) {
    this(options.dhtName, new ZooKeeperConfig(options.zkConfig), new DHTNodeConfiguration(),
        options.inactiveNodeTimeoutSeconds, options.getReapPolicy(), options.daemonPortOverride, options.daemonIP);
  }

  public final boolean getEnableMsgGroupTrace() {
    return enableMsgGroupTrace;
  }

  public void stop() {
    daemonStateZK.stopStateChecker();
    nodeInfoZK.stop();
    daemonStateTimer.purge();
    ConvergenceController2.cancelAllOngoingConvergence();

    msgModule.getStorage().stop();
    storageModuleTimer.purge();
    msgModule.stop();
    messageModuleTimer.purge();

    ringMaster.stop();
    running = false;
  }

  public void stopZk() {
    mc.closeZkExtendeed();
    ringMaster.stopMetaReaderZk();
  }

  private void cleanVM() {
    JVMUtil.getGlobalFinalization().forceFinalization(0);
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

  public ManagedStorageModule getManagedStorageModule() {
    return msgModule.getStorage();
  }

  public static void preSetup() {
    LWTPoolProvider.createDefaultWorkPools();
    Log.initAsyncLogging();
  }

  public static void postRun(boolean cleanReturn) {
    if (cleanReturn) {
      Log.warning("DHTNode run() returned cleanly");
      System.exit(0);
    } else {
      System.exit(-1);
    }
  }

  @FunctionalInterface
  public interface DHTNodeRunner {
    /**
     * @param givenNode     initialized DHTNode
     * @param givenNodePort the port used by the given node
     * @return true if the DHTNode::run() has clean return; false otherwise
     */
    boolean runDHTNode(DHTNode givenNode, int givenNodePort);
  }

  public static void withDhtNodeAndPort(DHTNodeOptions parsedOptions, DHTNodeRunner runner) {
    boolean cleanReturn;

    cleanReturn = false;
    try {
      DHTNode dhtNode;

      preSetup();
      dhtNode = new DHTNode(parsedOptions);
      cleanReturn = runner.runDHTNode(dhtNode, DHTNode.getActualPort());
    } catch (Throwable t) {
      Log.logErrorWarning(t);
      t.printStackTrace();
    } finally {
      Log.warning("DHTNode leaving withDhtNodeAndPort()");
    }

    postRun(cleanReturn);
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    DHTNodeOptions options;

    options = DHTNodeOptions.initialize(args);
    withDhtNodeAndPort(options, (givenNode, givenNodePort) -> {
      try {
        Log.warning("About to call DHTNode::run(), which uses port: " + givenNodePort);
        givenNode.run();
        return true;
      } catch (Throwable t) {
        Log.logErrorWarning(t, "Encountered exception during DHTNode::run()");
        t.printStackTrace();
        return false;
      }
    });
  }
}
