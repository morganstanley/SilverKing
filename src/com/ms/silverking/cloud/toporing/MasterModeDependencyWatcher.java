package com.ms.silverking.cloud.toporing;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.ms.silverking.cloud.dht.daemon.ReplicaNaiveIPPrioritizer;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.dht.management.LogStreamConfig;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.cloud.dht.meta.DHTRingCurTargetZK;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.meta.ServerSetExtensionZK;
import com.ms.silverking.cloud.meta.VersionListener;
import com.ms.silverking.cloud.meta.VersionWatcher;
import com.ms.silverking.cloud.topology.Topology;
import com.ms.silverking.cloud.topology.TopologyZK;
import com.ms.silverking.cloud.toporing.meta.MetaClient;
import com.ms.silverking.cloud.toporing.meta.MetaPaths;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfiguration;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfigurationUtil;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.log.Log;
import com.ms.silverking.thread.ThreadUtil;

/**
 * Watches ring dependencies and builds a new ring if any changes are detected.
 * Always builds with respect to a "master" ring.
 */
public class MasterModeDependencyWatcher implements VersionListener {
  private final MetaClient mc;
  private final MetaPaths mp;
  private final com.ms.silverking.cloud.dht.meta.MetaClient dhtMC;
  private final RingTree masterRingTree;
  private final Topology topology;
  private final long ringConfigVersion;
  private final NamedRingConfiguration ringConfig;
  private ResolvedReplicaMap existingReplicaMap;
  private final int consecutiveUpdateGuardSeconds;
  private final BlockingQueue<Map<String, Long>> buildQueue;
  private Map<String, Long> lastBuild;
  private final Set<String> updatesReceived;
  private final int _requiredInitialUpdates;

  public static final boolean verbose = true;

  private static final String logFileName = "MasterModeDependencyWatcher.out";

  private static final int requiredInitialUpdates = 2;

  public MasterModeDependencyWatcher(SKGridConfiguration gridConfig, MasterModeDependencyWatcherOptions options)
      throws IOException, KeeperException {
    ZooKeeperConfig zkConfig;
    long intervalMillis;
    long topologyVersion;
    long configInstanceVersion;
    RingTree existingTree;
    Pair<RingTree, Triple<String, Long, Long>> masterRingTreeReadPair;
    Triple<String, Long, Long> masterRingAndVersionPair;

    LogStreamConfig.configureLogStreams(gridConfig, logFileName);

    dhtMC = new com.ms.silverking.cloud.dht.meta.MetaClient(gridConfig);
    zkConfig = dhtMC.getZooKeeper().getZKConfig();
    ringConfig = NamedRingConfigurationUtil.fromGridConfiguration(gridConfig);
    mc = new MetaClient(ringConfig, zkConfig);
    mp = mc.getMetaPaths();

    _requiredInitialUpdates = options.ignoreInstanceExclusions ? requiredInitialUpdates - 1 : requiredInitialUpdates;

    consecutiveUpdateGuardSeconds = options.consecutiveUpdateGuardSeconds;
    buildQueue = new LinkedBlockingQueue<>();
    lastBuild = new HashMap<>();

    masterRingTreeReadPair = readMasterRingTree(dhtMC, dhtMC.getDHTConfiguration());
    masterRingTree = masterRingTreeReadPair.getV1();
    masterRingAndVersionPair = masterRingTreeReadPair.getV2();
    ringConfigVersion = masterRingAndVersionPair.getV2();

    topologyVersion = dhtMC.getZooKeeper().getLatestVersion(mp.getTopologyPath());
    topology = new TopologyZK(mc.createCloudMC()).readFromZK(topologyVersion, null);

    configInstanceVersion = dhtMC.getZooKeeper().getLatestVersion(mp.getConfigInstancePath(ringConfigVersion));
    existingTree = SingleRingZK.readTree(mc, ringConfigVersion, configInstanceVersion);
    existingReplicaMap = existingTree.getResolvedMap(ringConfig.getRingConfiguration().getRingParentName(),
        new ReplicaNaiveIPPrioritizer());

    /*
     * updatesReceived is used to ensure that we have an update from every version before we trigger a build
     */
    updatesReceived = new ConcurrentSkipListSet<>();

    intervalMillis = options.watchIntervalSeconds * 1000;
    // We don't need to watch everything that a full DependencyWatcher needs to watch
    new VersionWatcher(mc, mp.getExclusionsPath(), this, intervalMillis);
    new VersionWatcher(dhtMC, dhtMC.getMetaPaths().getInstanceExclusionsPath(), this, intervalMillis);

    new Builder();
  }

  private Pair<RingTree, Triple<String, Long, Long>> readMasterRingTree(
      com.ms.silverking.cloud.dht.meta.MetaClient dhtMC, DHTConfiguration dhtConfig)
      throws KeeperException, IOException {
    DHTRingCurTargetZK dhtRingCurTargetZK;
    Triple<String, Long, Long> masterRingAndVersionPair;
    RingTree ringTree;

    dhtRingCurTargetZK = new DHTRingCurTargetZK(dhtMC, dhtConfig);
    masterRingAndVersionPair = dhtRingCurTargetZK.getMasterRingAndVersionPair();
    if (masterRingAndVersionPair == null) {
      throw new RuntimeException("Can't find master ring");
    } else {
      ringTree = SingleRingZK.readTree(mc, masterRingAndVersionPair.getV2(), masterRingAndVersionPair.getV3());
      return new Pair<>(ringTree, masterRingAndVersionPair);
    }
  }

  @Override
  public void newVersion(String basePath, long version) {
    if (TopoRingConstants.verbose) {
      System.out.println("newVersion " + basePath + " " + version);
    }
    updatesReceived.add(basePath);
    if (updatesReceived.size() == _requiredInitialUpdates) {
      triggerBuild();
    }
  }

  private void triggerBuild() {
    try {
      ZooKeeperExtended zk;

      zk = mc.getZooKeeper();
      buildQueue.put(createBuildMap(zk));
    } catch (Exception e) {
      Log.logErrorWarning(e);
    }
  }

  private Map<String, Long> createBuildMap(ZooKeeperExtended zk) throws KeeperException {
    Map<String, Long> b;
    long exclusionVersion;
    long instanceExclusionVersion;

    exclusionVersion = zk.getLatestVersion(mp.getExclusionsPath());
    instanceExclusionVersion = zk.getLatestVersion(dhtMC.getMetaPaths().getInstanceExclusionsPath());

    b = new HashMap<>();
    b.put(mp.getExclusionsPath(), exclusionVersion);
    b.put(dhtMC.getMetaPaths().getInstanceExclusionsPath(), instanceExclusionVersion);
    return b;
  }

  /**
   * Build a new ring based off of the master ring
   *
   * @param curBuild
   */
  private void build(Map<String, Long> curBuild) {
    try {
      ExclusionSet exclusionSet;
      ExclusionSet instanceExclusionSet;
      long exclusionVersion;
      long instanceExclusionVersion;
      ZooKeeperExtended zk;
      ExclusionSet mergedExclusionSet;
      RingTree newRingTree;
      String newInstancePath;
      ResolvedReplicaMap newReplicaMap;

      zk = mc.getZooKeeper();
      exclusionVersion = curBuild.get(mp.getExclusionsPath());
      instanceExclusionVersion = curBuild.get(dhtMC.getMetaPaths().getInstanceExclusionsPath());

      exclusionSet = new ExclusionSet(
          new ServerSetExtensionZK(mc, mc.getMetaPaths().getExclusionsPath()).readFromZK(exclusionVersion, null));
      try {
        instanceExclusionSet = new ExclusionSet(
            new ServerSetExtensionZK(mc, dhtMC.getMetaPaths().getInstanceExclusionsPath()).readFromZK(
                instanceExclusionVersion, null));
      } catch (Exception e) {
        Log.warning("No instance ExclusionSet found");
        instanceExclusionSet = ExclusionSet.emptyExclusionSet(0);
      }
      mergedExclusionSet = ExclusionSet.union(exclusionSet, instanceExclusionSet);
      newRingTree = RingTreeBuilder.removeExcludedNodes(masterRingTree, mergedExclusionSet);

      newReplicaMap = newRingTree.getResolvedMap(ringConfig.getRingConfiguration().getRingParentName(),
          new ReplicaNaiveIPPrioritizer());
      if (!existingReplicaMap.equals(newReplicaMap)) {
        newInstancePath = mc.createConfigInstancePath(ringConfigVersion);
        SingleRingZK.writeTree(mc, topology, newInstancePath, newRingTree);
        Log.warningf("RingTree written to ZK: %s", newInstancePath);
        existingReplicaMap = newReplicaMap;
      } else {
        Log.warning("RingTree unchanged. No ZK update.");
      }
    } catch (Exception e) {
      e.printStackTrace();
      Log.logErrorWarning(e, "handleExclusionChange() failed");
    }
  }

  private class Builder implements Runnable {
    Builder() {
      new Thread(this).start();
    }

    @Override
    public void run() {
      while (true) {
        try {
          Map<String, Long> curBuild;
          Map<String, Long> _curBuild;

          curBuild = buildQueue.take();
          Log.warning("Received new build");
          Log.warning("Checking for consecutive update");
          _curBuild = buildQueue.poll(consecutiveUpdateGuardSeconds, TimeUnit.SECONDS);
          while (_curBuild != null) {
            Log.warning("Received new build consecutively. Ignoring last received.");
            Log.warning("Checking for consecutive update");
            curBuild = _curBuild;
            _curBuild = buildQueue.poll(consecutiveUpdateGuardSeconds, TimeUnit.SECONDS);
          }
          if (!lastBuild.equals(curBuild)) {
            build(curBuild);
            lastBuild = curBuild;
          }
        } catch (Exception e) {
          Log.logErrorWarning(e);
          ThreadUtil.pauseAfterException();
        }
      }
    }
  }

  public static void main(String[] args) {
    try {
      MasterModeDependencyWatcherOptions options;
      CmdLineParser parser;

      options = new MasterModeDependencyWatcherOptions();
      parser = new CmdLineParser(options);
      try {
        MasterModeDependencyWatcher dw;
        SKGridConfiguration gc;

        parser.parseArgument(args);
        TopoRingConstants.setVerbose(true);
        gc = SKGridConfiguration.parseFile(options.gridConfig);
        dw = new MasterModeDependencyWatcher(gc, options);
        ThreadUtil.sleepForever();
      } catch (CmdLineException cle) {
        System.err.println(cle.getMessage());
        parser.printUsage(System.err);
        System.exit(-1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
