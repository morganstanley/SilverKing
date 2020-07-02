package com.ms.silverking.cloud.dht.client;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.NamespaceOptionsMode;
import com.ms.silverking.cloud.dht.daemon.DHTNode;
import com.ms.silverking.cloud.dht.daemon.DHTNodeConfiguration;
import com.ms.silverking.cloud.dht.daemon.storage.ReapOnIdlePolicy;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.cloud.dht.meta.DHTConfigurationZK;
import com.ms.silverking.cloud.dht.meta.DHTRingCurTargetZK;
import com.ms.silverking.cloud.dht.meta.MetaClient;
import com.ms.silverking.cloud.toporing.StaticRingCreator;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfiguration;
import com.ms.silverking.cloud.zookeeper.LocalZKImpl;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.thread.lwt.LWTPoolProvider;

public class EmbeddedSK {
  private static AtomicBoolean embeddedExist = new AtomicBoolean();
  private static ConcurrentMap<String, NamedRingConfiguration> namedRingConfigs = new ConcurrentHashMap<>();

  public static boolean embedded() {
    return embeddedExist.get();
  }

  public static NamedRingConfiguration getEmbeddedNamedRingConfiguration(String ringName) {
    return namedRingConfigs.get(ringName);
  }

  public static void setEmbeddedNamedRingConfiguration(String ringName, NamedRingConfiguration namedRingConfig) {
    embeddedExist.set(true);
    namedRingConfigs.put(ringName, namedRingConfig);
  }
    
    /*
     Could leave this for backwards compatibility, but best to remove
    public static ClientDHTConfiguration createEmbeddedSKInstance(String dhtName, String gridConfigName, String
    ringName, int replication, NamespaceOptionsMode nsOptionsMode) {
        EmbeddedSKConfiguration config;
        
        config = new EmbeddedSKConfiguration(dhtName, gridConfigName, ringName, replication); 
        return createEmbeddedSKInstance(config);
    }
    */

  public static ClientDHTConfiguration createEmbeddedSKInstance(EmbeddedSKConfiguration config) {
    try {
      int zkPort;
      Path tempDir;
      File zkDir;
      File skDir;
      ZooKeeperConfig zkConfig;

      // 0) Create LWT work pools
      LWTPoolProvider.createDefaultWorkPools();

      // 1) Start an embedded ZooKeeper
      Log.warning("Creating embedded ZooKeeper");
      try {
        tempDir = Files.createTempDirectory(null);
        tempDir.toFile().deleteOnExit();
        zkDir = new File(tempDir.toFile(), "zookeeper");
        zkDir.mkdirs();
        skDir = new File(tempDir.toFile(), "silverking");
        skDir.mkdirs();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
      zkPort = LocalZKImpl.startLocalZK(zkDir.getAbsolutePath());
      zkConfig = new ZooKeeperConfig(InetAddress.getLoopbackAddress().getHostAddress() + ":" + zkPort);
      Log.warning("Embedded ZooKeeper running at: " + zkConfig);

      DHTNodeConfiguration nodeConfig = new DHTNodeConfiguration(skDir.getAbsolutePath() + "/data");

      // 2) Create ring in ZK
      Log.warning("Creating ring");
      StaticRingCreator.createStaticRing(config.getRingName(), zkConfig, ImmutableSet.of(IPAddrUtil.localIPString()),
          config.getReplication());
      Log.warning("Created: " + config.getRingName());

      // 3) Create DHT Config in ZK
      DHTConfiguration dhtConfig;
      MetaClient dhtMC;
      DHTConfigurationZK dhtConfigZK;
      ClientDHTConfiguration clientDHTConfig;
      int dhtPort;

      Log.warning("Creating DHT configuration in ZK");
      if (config.getDhtPort() <= 0) {
        dhtPort = ThreadLocalRandom.current().nextInt(10000, 20000); // FIXME
      } else {
        dhtPort = config.getDhtPort();
      }
      clientDHTConfig = new ClientDHTConfiguration(config.getDHTName(), dhtPort, zkConfig);
      dhtMC = new MetaClient(clientDHTConfig);
      dhtConfigZK = new DHTConfigurationZK(dhtMC);
      dhtConfig = DHTConfiguration.emptyTemplate.ringName(config.getRingName()).port(dhtPort).passiveNodeHostGroups(
          "").hostGroupToClassVarsMap(config.getClassVars()).namespaceOptionsMode(
          config.getNamespaceOptionsMode()).enableMsgGroupTrace(config.getEnableMsgGroupTrace());

      dhtConfigZK.writeToZK(dhtConfig, null);
      Log.warning("Created DHT configuration in ZK");

      // 4) Set cur and target rings
      DHTRingCurTargetZK curTargetZK;

      Log.warning("Setting ring targets");
      curTargetZK = new DHTRingCurTargetZK(dhtMC, dhtConfig);
      curTargetZK.setCurRingAndVersionPair(config.getRingName(), 0, 0);
      curTargetZK.setTargetRingAndVersionPair(config.getRingName(), 0, 0);
      Log.warning("Ring targets set");

      // 4) Start DHTNode
      Log.warning("Starting DHTNode");
      new DHTNode(config.getDHTName(), zkConfig, nodeConfig, 0, new ReapOnIdlePolicy(), DHTConstants.noPortOverride,
          config.getDaemonIp());
      Log.warning("DHTNode started");

      // 5) Return the configuration to the caller
      return clientDHTConfig;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

    /*
     * Could leave these for backwards compatibility, but best to remove.
    public static ClientDHTConfiguration createEmbeddedSKInstance(String id, int replication, NamespaceOptionsMode
    * nsOptionsMode) {
        return createEmbeddedSKInstance(new EmbeddedSKConfiguration(id, replication, nsOptionsMode));
    }
    
    public static ClientDHTConfiguration createEmbeddedSKInstance(String id, NamespaceOptionsMode nsOptionsMode) {
        return createEmbeddedSKInstance(new EmbeddedSKConfiguration(id, nsOptionsMode));
    }
    
    public static ClientDHTConfiguration createEmbeddedSKInstance(int replication, NamespaceOptionsMode nsOptionsMode) {
        return createEmbeddedSKInstance(new EmbeddedSKConfiguration(replication, nsOptionsMode));
    }

    public static ClientDHTConfiguration createEmbeddedSKInstance(NamespaceOptionsMode nsOptionsMode) {
        return createEmbeddedSKInstance(new EmbeddedSKConfiguration(new UUIDBase(false).toString(), nsOptionsMode));
    }
    */

  public static ClientDHTConfiguration createEmbeddedSKInstance() {
    return createEmbeddedSKInstance(new EmbeddedSKConfiguration());
  }

  public static void main(String[] args) {
    if (args.length > 1) {
      System.out.println("args: [ZooKeeper|NSP]");
    } else {
      try {
        ClientDHTConfiguration dhtConfig;
        NamespaceOptionsMode nsOptionsMode;

        nsOptionsMode = args.length == 0 ? DHTConfiguration.defaultNamespaceOptionsMode : NamespaceOptionsMode.valueOf(
            args[0]);
        dhtConfig = createEmbeddedSKInstance(new EmbeddedSKConfiguration().namespaceOptionsMode(nsOptionsMode));
        System.out.printf("DHT Configuration: %s\n", dhtConfig);
        ThreadUtil.sleepForever();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

}
