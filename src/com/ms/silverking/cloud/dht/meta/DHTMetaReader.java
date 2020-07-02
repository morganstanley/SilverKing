package com.ms.silverking.cloud.dht.meta;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.dht.client.EmbeddedSK;
import com.ms.silverking.cloud.meta.CloudConfiguration;
import com.ms.silverking.cloud.toporing.InstantiatedRingTree;
import com.ms.silverking.cloud.toporing.SingleRingZK;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfiguration;
import com.ms.silverking.cloud.toporing.meta.RingConfiguration;
import com.ms.silverking.cloud.toporing.meta.RingConfigurationZK;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.log.Log;
import com.ms.silverking.thread.ThreadUtil;

/**
 * Reads new rings when necessary. Presents these as DHTMetaUpdates for compatibility with
 * legacy code.
 */
public class DHTMetaReader {
  private final MetaClient mc;
  private final MetaPaths mp;
  private final ZooKeeperConfig zkConfig;
  private final boolean enableLogging;
  private DHTConfiguration dhtConfig;

  public DHTMetaReader(ZooKeeperConfig zkConfig, String dhtName, boolean enableLogging)
      throws IOException, KeeperException {
    mc = new MetaClient(dhtName, zkConfig);
    mp = mc.getMetaPaths();
    this.zkConfig = zkConfig;
    this.enableLogging = enableLogging;
    dhtConfig = mc.getDHTConfiguration();
  }

  public DHTMetaReader(ZooKeeperConfig zkConfig, String dhtName) throws IOException, KeeperException {
    this(zkConfig, dhtName, true);
  }

  public MetaClient getMetaClient() {
    return mc;
  }

  public void setDHTConfig(DHTConfiguration dhtConfig) {
    this.dhtConfig = dhtConfig;
  }

  public DHTConfiguration getDHTConfig() {
    return dhtConfig;
  }

  public DHTMetaUpdate readRing(String curRing, Pair<Long, Long> ringVersionPair) throws KeeperException, IOException {
    return readRing(curRing, ringVersionPair.getV1(), ringVersionPair.getV2());
  }

  public DHTMetaUpdate readRing(Triple<String, Long, Long> ringAndVersionPair) throws KeeperException, IOException {
    return readRing(ringAndVersionPair.getV1(), ringAndVersionPair.getV2(), ringAndVersionPair.getV3());
  }

  public DHTMetaUpdate readRing(String ringName, long ringConfigVersion, long configInstanceVersion)
      throws KeeperException, IOException {
    com.ms.silverking.cloud.toporing.meta.MetaClient ringMC;
    NamedRingConfiguration namedRingConfig;
    RingConfiguration ringConfig;
    InstantiatedRingTree ringTree;
    //        long                ringConfigVersion;
    //        long                configInstanceVersion;
    int readAttemptIndex;
    int ringReadAttempts = 20;
    int ringReadRetryInvervalSeconds = 2;
    ZooKeeperExtended zk;

    zk = mc.getZooKeeper();

    if (EmbeddedSK.embedded()) {
      namedRingConfig = EmbeddedSK.getEmbeddedNamedRingConfiguration(ringName);
    } else {
      // unresolved
      namedRingConfig = new NamedRingConfiguration(ringName, RingConfiguration.emptyTemplate);
      ringMC = new com.ms.silverking.cloud.toporing.meta.MetaClient(namedRingConfig, zkConfig);
      //            ringConfigVersion = zk.getVersionPriorTo(ringMC.getMetaPaths().getConfigPath(), zkidLimit);
      try {
        ringConfig = new RingConfigurationZK(ringMC).readFromZK(ringConfigVersion, null);
      } catch (Exception e) {
        Log.warning("Ignoring: ", e);
        ringConfig = new RingConfiguration(new CloudConfiguration(null, null, null), null, null, null, null, null);
      }

      // resolved
      namedRingConfig = new NamedRingConfiguration(ringName, ringConfig);
      if (enableLogging) {
        Log.warning("ringConfig\t", ringConfig);
      }
    }
    ringMC = new com.ms.silverking.cloud.toporing.meta.MetaClient(namedRingConfig, zkConfig);

    //        configInstanceVersion = zk.getVersionPriorTo(ringMC.getMetaPaths().getConfigInstancePath
    //        (ringConfigVersion), zkidLimit);
    if (enableLogging) {
      Log.warning("configInstanceVersion:: " + configInstanceVersion);
    }
    if (configInstanceVersion < 0) {
      throw new RuntimeException("Invalid configInstanceVersion: " + configInstanceVersion);
    }
    //        if (configInstanceVersion == -1) {
    //            configInstanceVersion = 0;
    //        }

    // FUTURE - we shouldn't get here unless it's valid. Think about error messages if invalid, instead of waiting.
    Log.warning("Waiting until valid " + ringMC.getMetaPaths().getConfigInstancePath(
        ringConfigVersion) + " " + configInstanceVersion);
    SingleRingZK.waitUntilValid(ringMC, ringMC.getMetaPaths().getConfigInstancePath(ringConfigVersion),
        configInstanceVersion);
    Log.warning("Valid");

    ringTree = null;
    readAttemptIndex = 0;
    while (ringTree == null) {
      try {
        ringTree = SingleRingZK.readTree(ringMC, ringConfigVersion, configInstanceVersion);//, weightsVersion);
      } catch (Exception e) {
        if (++readAttemptIndex >= ringReadAttempts) {
          throw new RuntimeException("Ring read failed", e);
        } else {
          ThreadUtil.sleepSeconds(ringReadRetryInvervalSeconds);
        }
      }
    }
    if (enableLogging) {
      Log.warning("\t\t###\t" + ringConfigVersion + "\t" + configInstanceVersion);
    }
    return new DHTMetaUpdate(dhtConfig, namedRingConfig, ringTree, mc);
  }

    /*
    public static void main(String[] args) {
        try {
            if (args.length != 3) {
                System.out.println("args: <zkConfig> <mapConfig> <intervalSeconds>>");
            } else {
                ZooKeeperConfig zkConfig;
                DHTMetaReader  dw;
                String          dhtName;
                long            intervalMillis;
                
                zkConfig = new ZooKeeperConfig(args[0]);
                dhtName = args[1];
                intervalMillis = Integer.parseInt(args[2]) * 1000;
                dw = new DHTMetaReader(zkConfig, dhtName, intervalMillis);
                dw.addListener(new RingUpdateListenerTest());
                Thread.sleep(60 * 60 * 1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    */
}
