package com.ms.silverking.cloud.dht.client;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.dht.SessionOptions;
import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.cloud.dht.client.impl.DHTSessionImpl;
import com.ms.silverking.cloud.dht.client.serialization.SerializationRegistry;
import com.ms.silverking.cloud.dht.common.NamespaceOptionsMode;
import com.ms.silverking.cloud.dht.common.SimpleValueCreator;
import com.ms.silverking.cloud.dht.daemon.DHTNode;
import com.ms.silverking.cloud.dht.daemon.DHTNodeConfiguration;
import com.ms.silverking.cloud.dht.daemon.storage.NeverReapPolicy;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.cloud.dht.meta.DHTConfigurationZK;
import com.ms.silverking.cloud.dht.meta.MetaClient;
import com.ms.silverking.cloud.dht.meta.MetaPaths;
import com.ms.silverking.cloud.dht.meta.StaticDHTCreator;
import com.ms.silverking.cloud.dht.net.IPAliasMap;
import com.ms.silverking.cloud.dht.net.IPAliasingUtil;
import com.ms.silverking.cloud.toporing.TopoRingConstants;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.net.async.AsyncGlobals;
import com.ms.silverking.net.async.OutgoingData;
import com.ms.silverking.thread.lwt.DefaultWorkPoolParameters;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.time.AbsMillisTimeSource;
import com.ms.silverking.time.TimerDrivenTimeSource;

/**
 * Base client interface to DHT functionality. Provides sessions
 * to specific DHT instances.
 */
public class DHTClient {
  private final SerializationRegistry serializationRegistry;

  private static final double concurrentExtraThreadFactor = 1.25;
  private static final double nonConcurrentExtraThreadFactor = 1.0;

  private static ValueCreator valueCreator;

  private static final AbsMillisTimeSource absMillisTimeSource;

  private static final int defaultInactiveNodeTimeoutSeconds = 30;
    
    /*
    private static void initLog4j() {  
        Logger rootLogger = Logger.getRootLogger();  
        if (!rootLogger.getAllAppenders().hasMoreElements()) {      
            rootLogger.setLevel(org.apache.log4j.Level.WARN);      
            rootLogger.addAppender(new ConsoleAppender(             
                    new PatternLayout("%-5p [%t]: %m%n")));
        }
    }
    */

  private static final int defaultClientWorkUnit = 16;

  static {
    Log.initAsyncLogging();
    AsyncGlobals.setVerbose(false);
    TopoRingConstants.setVerbose(false);
    LWTPoolProvider.createDefaultWorkPools(
        DefaultWorkPoolParameters.defaultParameters().workUnit(defaultClientWorkUnit).ignoreDoubleInit(true));
    //initLog4j();
    //WorkPoolProvider.createWorkPools(concurrentExtraThreadFactor, nonConcurrentExtraThreadFactor, true);
    //if (!Log.levelMet(Level.INFO)) {
    //    AsyncServer.verbose = false;
    //}
    valueCreator = SimpleValueCreator.forLocalProcess();
    absMillisTimeSource = new TimerDrivenTimeSource();
    OutgoingData.setAbsMillisTimeSource(absMillisTimeSource);
  }

  /**
   * Return the ValueCreator in use
   *
   * @return the ValueCreator in use
   */
  public static ValueCreator getValueCreator() {
    return valueCreator;
  }

  /**
   * Construct DHTClient with the specified SerializationRegistry.
   *
   * @param serializationRegistry
   * @throws IOException
   */
  @OmitGeneration
  public DHTClient(SerializationRegistry serializationRegistry) throws IOException {
    this.serializationRegistry = serializationRegistry;
  }

  /**
   * Construct DHTClient with default SerializationRegistry.
   *
   * @throws IOException
   */
  public DHTClient() throws IOException {
    this(SerializationRegistry.createDefaultRegistry());
  }

  /**
   * Open a new session to the specified SilverKing DHT instance using default SessionOptions.
   *
   * @param dhtConfigProvider specifies the SilverKing DHT instance
   * @return a new session to the given instance
   * @throws ClientException
   */
  public DHTSession openSession(ClientDHTConfigurationProvider dhtConfigProvider) throws ClientException {
    return openSession(new SessionOptions(dhtConfigProvider.getClientDHTConfiguration()));
  }

  /**
   * Open a new session to the specified SilverKing DHT instance using the given SessionOptions.
   *
   * @param sessionOptions options specifying the SilverKing DHT instance and the parameters of this session
   * @return a new session to the given instance
   * @throws ClientException
   */
  public DHTSession openSession(SessionOptions sessionOptions) throws ClientException {
    ClientDHTConfiguration dhtConfig;
    String preferredServer;
    NamespaceOptionsMode nsOptionsMode;
    boolean enableMsgGroupTrace;
    DHTSession session;
    int serverPort;

    dhtConfig = sessionOptions.getDHTConfig();
    preferredServer = sessionOptions.getPreferredServer();

    if (preferredServer != null) {
      // Handle cases where a reserved preferredServer is used to indicate that an embedded SK
      // instance is desired
      if (preferredServer.equals(SessionOptions.EMBEDDED_PASSIVE_NODE)) {
        embedPassiveNode(dhtConfig);
        // Assumption: There will be DHTConfig registered in ZK for this EMBEDDED_PASSIVE_NODE
        try {
          DHTConfiguration dhtGlobalConfig;

          dhtGlobalConfig = new MetaClient(dhtConfig).getDHTConfiguration();
          nsOptionsMode = dhtGlobalConfig.getNamespaceOptionsMode();
          enableMsgGroupTrace = dhtGlobalConfig.getEnableMsgGroupTrace();
        } catch (KeeperException | IOException e) {
          throw new ClientException("Failed to read global DHTConfiguration", e);
        }
        preferredServer = null;
      } else if (preferredServer.equals(SessionOptions.EMBEDDED_KVS)) {
        File gcBase;
        String gcName;

        // Assumption: There could be no DHTConfig registered in ZK for this EMBEDDED_KVS
        // NOTE: if Embedded is created via openSession API, default nsOptionsMode is used
        nsOptionsMode = DHTConfiguration.defaultNamespaceOptionsMode;
        enableMsgGroupTrace = DHTConfiguration.defaultEnableMsgGroupTrace;
        dhtConfig = embedKVS(nsOptionsMode, enableMsgGroupTrace);
        try {
          gcBase = Files.createTempDirectory("embeddedSKGC").toFile(); // FIXME - make user configurable
          gcBase.deleteOnExit();
        } catch (IOException ioe) {
          throw new ClientException("Failed to create temp directory for EMBEDDED_KVS", ioe);
        }
        gcName = "GC_" + dhtConfig.getName();
        try {
          Log.warningf("GridConfigBase: %s", gcBase);
          Log.warningf("GridConfigName: %s", gcName);
          StaticDHTCreator.writeGridConfig(dhtConfig, gcBase, gcName);
        } catch (IOException e) {
          throw new ClientException("Error creating embedded kvs", e);
        }
        preferredServer = null;
      } else {
        // Normal openSession must have nsOptionsMode
        try {
          DHTConfiguration dhtGlobalConfig;

          dhtGlobalConfig = new MetaClient(dhtConfig).getDHTConfiguration();
          nsOptionsMode = dhtGlobalConfig.getNamespaceOptionsMode();
          enableMsgGroupTrace = dhtGlobalConfig.getEnableMsgGroupTrace();
        } catch (KeeperException | IOException e) {
          throw new ClientException("Failed to read global DHTConfiguration", e);
        }
      }
    } else {
      // This is a typical openSession - not embedded
      // Normal openSession must have nsOptionsMode
      try {
        DHTConfiguration dhtGlobalConfig;

        dhtGlobalConfig = new MetaClient(dhtConfig).getDHTConfiguration();
        nsOptionsMode = dhtGlobalConfig.getNamespaceOptionsMode();
        enableMsgGroupTrace = dhtGlobalConfig.getEnableMsgGroupTrace();
      } catch (KeeperException | IOException e) {
        throw new ClientException("Failed to read global DHTConfiguration", e);
      }
    }

    // Determine the serverPort
    try {
      if (dhtConfig.hasPort()) { // Best to avoid zk if possible
        serverPort = dhtConfig.getPort();
      } else { // not specified, must contact zk
        MetaClient mc;
        MetaPaths mp;
        long latestConfigVersion;

        Log.warning("dhtConfig.getZkLocs(): " + dhtConfig.getZKConfig());
        mc = new MetaClient(dhtConfig);
        mp = mc.getMetaPaths();

        Log.warning("getting latest version: " + mp.getInstanceConfigPath());
        latestConfigVersion = mc.getZooKeeper().getLatestVersion(mp.getInstanceConfigPath());
        Log.warning("latestConfigVersion: " + latestConfigVersion);
        serverPort = new DHTConfigurationZK(mc).readFromZK(latestConfigVersion, null).getPort();
      }
    } catch (Exception e) {
      throw new ClientException(e);
    }

    // Now determine the daemon ip and port, and open a session
    try {
      IPAndPort   preferredServerAndPort;
      IPAndPort   resolvedServer;
      IPAliasMap  aliasMap;

      // See IPAliasMap for discussion of the aliasing, daemon ip:port, and interface ip:port

      aliasMap = IPAliasingUtil.readAliases(dhtConfig);

      // Handle case of no preferred server explicitly specified
      if (preferredServer == null) {
        // When no preferred server is specified, we default to the local host
        preferredServer = IPAddrUtil.localIPString();
        Log.infof("No preferred server specified. Using: %s", preferredServer);
        // If any aliases are defined for the local server, randomly select one
        resolvedServer = aliasMap.interfaceIPToRandomDaemon(preferredServer);
        if (resolvedServer != null) {
          Log.infof("Found alias(es) for local server; randomly selected: %s", resolvedServer);
        } else {
          resolvedServer = new IPAndPort(IPAddrUtil.serverNameToAddr(preferredServer), serverPort);
        }
      } else {
        // A preferred server was explicitly specified
        preferredServerAndPort = new IPAndPort(IPAddrUtil.serverNameToAddr(preferredServer), serverPort);
        Log.infof("preferredServerAndPort: %s", preferredServerAndPort);
        // Check if we need to resolve this daemon to an interface
        resolvedServer = (IPAndPort)aliasMap.daemonToInterface(preferredServerAndPort);
        if (resolvedServer != preferredServerAndPort) {
          Log.infof("Found alias for %s -> %s", preferredServerAndPort, resolvedServer);
        }
      }

      Log.infof("Opening session to resolvedServer: %s", resolvedServer);
      session = new DHTSessionImpl(dhtConfig,
          resolvedServer, absMillisTimeSource,
          serializationRegistry, sessionOptions.getTimeoutController(), nsOptionsMode, enableMsgGroupTrace,
          aliasMap);
    } catch (IOException ioe) {
      throw new ClientException(ioe);
    }
    Log.info("session returned: ", session);
    return session;
  }

  private void embedPassiveNode(ClientDHTConfiguration dhtConfig) {
    DHTNode embeddedNode;
    Path tempDir;
    File skDir;

    try {
      tempDir = Files.createTempDirectory(null);
      skDir = new File(tempDir.toFile(), "silverking");
      skDir.mkdirs();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }

    DHTNodeConfiguration nodeConfig = new DHTNodeConfiguration(skDir.getAbsolutePath() + "/data");
    embeddedNode = new DHTNode(dhtConfig.getName(), dhtConfig.getZKConfig(), nodeConfig,
        defaultInactiveNodeTimeoutSeconds, NeverReapPolicy.instance, dhtConfig.getPort(), null);
  }

  private ClientDHTConfiguration embedKVS(NamespaceOptionsMode nsOptionsMode, boolean enableMsgGroupTrace) {
    return EmbeddedSK.createEmbeddedSKInstance(
        new EmbeddedSKConfiguration().namespaceOptionsMode(nsOptionsMode).enableMsgGroupTrace(enableMsgGroupTrace));
  }
}
