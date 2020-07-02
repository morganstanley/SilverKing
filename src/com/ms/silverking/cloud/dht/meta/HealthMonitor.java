package com.ms.silverking.cloud.dht.meta;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ms.silverking.util.PropertiesHelper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.RingHealth;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.management.RingIntegrityCheck;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.dht.management.LogStreamConfig;
import com.ms.silverking.cloud.meta.ChildrenListener;
import com.ms.silverking.cloud.meta.ChildrenWatcher;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.meta.ExclusionZK;
import com.ms.silverking.cloud.toporing.InstantiatedRingTree;
import com.ms.silverking.cloud.toporing.ResolvedReplicaMap;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfiguration;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfigurationUtil;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.time.TimeUtils;

public class HealthMonitor implements ChildrenListener, DHTMetaUpdateListener {
  private final SKGridConfiguration gc;
  private final ZooKeeperConfig zkConfig;
  private final MetaClient mc;
  private final SuspectsZK suspectsZK;
  private final DaemonStateZK daemonStateZK;
  private final int watchIntervalSeconds;
  private ChildrenWatcher watcher;
  private final DHTMetaWatcher dmw;
  private final int guiltThreshold;
  private boolean running;
  private ExclusionZK cloudExclusionZK;
  private InstanceExclusionZK instanceExclusionZK;
  private DHTRingCurTargetZK dhtRingCurTargetZK;
  private Set<IPAndPort> activeNodes;
  private Lock checkMutex;
  private final Doctor doctor;
  private final DoctorRunner doctorRunner;
  private final int doctorRoundIntervalSeconds;
  private final boolean forceInclusionOfUnsafeExcludedServers;
  private final ConvictionLimits convictionLimits;
  private final ConvictionLimits convictionWarningThresholds;
  private final Map<IPAndPort, Long> convictionTimes;
  private volatile long lastCheckMillis;
  private volatile long lastUpdateMillis;
  private final long minUpdateIntervalMillis;
  private Set<IPAndPort> activeNodesInMap;
  private final boolean disableAddition;
  private final RingIntegrityCheck ringIntegrityCheck;
  private boolean ringHealthNodeCheckedForInitialization;

  private static final String logFileName = "HealthMonitor.out";
  private static final String doctorThreadName = "DoctorRunner";

  //private static final int    dmwUpdateIntervalMillis = 5 * 1000; // for testing
  private static final int dmwUpdateIntervalMillis = 1 * 60 * 1000;
  //private static final int    inactiveNodeMarkingThreshold_servers = 1;
  //private static final double inactiveNodeMarkingThreshold_fraction = 0.05;
  private static final int forcedCheckIntervalMillis = 1 * 60 * 1000;

  private static final long oneHourMillis =
      TimeUtils.MINUTES_PER_HOUR * TimeUtils.SECONDS_PER_MINUTE * TimeUtils.MILLIS_PER_SECOND;

  private static final long max_minUpdateIntervalMillis =
      10 * TimeUtils.SECONDS_PER_MINUTE * TimeUtils.MILLIS_PER_SECOND; // used as a sanity check on updates

  // FUTURE: just pass in the options...
  public HealthMonitor(SKGridConfiguration gc, ZooKeeperConfig zkConfig, int watchIntervalSeconds, int guiltThreshold,
      int doctorRoundIntervalSeconds, boolean forceInclusionOfUnsafeExcludedServers, ConvictionLimits convictionLimits,
      ConvictionLimits convictionWarningThresholds, int doctorNodeStartupTimeoutSeconds, boolean disableAddition,
      long minUpdateIntervalMillis) throws IOException, KeeperException, ClientException {
    String dhtName;
    com.ms.silverking.cloud.meta.MetaClient cloudMC;

    this.gc = gc;
    dhtName = gc.getClientDHTConfiguration().getName();
    ResolvedReplicaMap.setDHTPort(gc.getClientDHTConfiguration().getPort());
    this.guiltThreshold = guiltThreshold;
    this.zkConfig = zkConfig;
    this.watchIntervalSeconds = watchIntervalSeconds;
    this.doctorRoundIntervalSeconds = doctorRoundIntervalSeconds;
    this.forceInclusionOfUnsafeExcludedServers = forceInclusionOfUnsafeExcludedServers;
    this.convictionLimits = convictionLimits;
    this.convictionWarningThresholds = convictionWarningThresholds;
    convictionTimes = new HashMap<>();
    this.disableAddition = disableAddition;
    this.minUpdateIntervalMillis = minUpdateIntervalMillis;

    Log.warningf("guiltThreshold %d", guiltThreshold);
    Log.warningf("watchIntervalSeconds %d", watchIntervalSeconds);
    Log.warningf("doctorRoundIntervalSeconds %d", doctorRoundIntervalSeconds);
    Log.warningf("convictionLimits %s", convictionLimits);
    Log.warningf("convictionWarningThresholds %s", convictionWarningThresholds);
    Log.warningf("doctorNodeStartupTimeoutSeconds %d", doctorNodeStartupTimeoutSeconds);
    Log.warningf("disableAddition %s", disableAddition);
    Log.warningf("minUpdateIntervalMillis %d", minUpdateIntervalMillis);
    if (doctorRoundIntervalSeconds != HealthMonitorOptions.NO_DOCTOR) {
      doctor = new Doctor(gc, forceInclusionOfUnsafeExcludedServers, doctorNodeStartupTimeoutSeconds);
    } else {
      doctor = null;
    }
    activeNodes = ImmutableSet.of();
    checkMutex = new ReentrantLock();
    mc = new MetaClient(dhtName, zkConfig);
    suspectsZK = new SuspectsZK(mc);
    daemonStateZK = new DaemonStateZK(mc);

    NamedRingConfiguration ringConfig;
    ringConfig = NamedRingConfigurationUtil.fromGridConfiguration(gc);
    cloudMC = new com.ms.silverking.cloud.meta.MetaClient(ringConfig.getRingConfiguration().getCloudConfiguration(),
        gc.getClientDHTConfiguration().getZKConfig());

    cloudExclusionZK = new ExclusionZK(cloudMC);
    dmw = new DHTMetaWatcher(zkConfig, dhtName, dmwUpdateIntervalMillis);
    dmw.addListener(this);
    if (doctor != null) {
      doctorRunner = new DoctorRunner();
      doctorRunner.start();
    } else {
      doctorRunner = null;
    }

    ringIntegrityCheck = new RingIntegrityCheck(gc);
  }

  @Override
  public void dhtMetaUpdate(DHTMetaUpdate dhtMetaUpdate) {
    Log.warning(String.format("Received dhtMetaUpdate %s", dhtMetaUpdate));
    try {
      InstantiatedRingTree rawRingTree;
      ResolvedReplicaMap replicaMap;

      instanceExclusionZK = new InstanceExclusionZK(dhtMetaUpdate.getMetaClient());
      dhtRingCurTargetZK = new DHTRingCurTargetZK(dhtMetaUpdate.getMetaClient(), dhtMetaUpdate.getDHTConfig());
      rawRingTree = dhtMetaUpdate.getRingTree();
      replicaMap = rawRingTree.getResolvedMap(
          dhtMetaUpdate.getNamedRingConfiguration().getRingConfiguration().getRingParentName(), null);
      activeNodesInMap = replicaMap.allReplicas();
      Log.warningf("activeNodesInMap %s", activeNodesInMap);
      synchronized (this) {
        this.notifyAll();
      }
    } catch (Exception e) {
      Log.logErrorWarning(e, "Exception in HealthMonitor.dhtMetaUpdate()");
    }
  }

  @Override
  public void childrenChanged(String basePath, Map<String, byte[]> childStates) {
    Log.warning(String.format("\nchildrenChanged\n"));
    for (Map.Entry<String, byte[]> entry : childStates.entrySet()) {
      Log.warning(String.format("%s\t%s", entry.getKey(), new String(entry.getValue())));
    }
    check();
  }

  private void verifyEligibility(Set<IPAndPort> activeNodes) throws KeeperException {
    if (activeNodes.size() > 0) {
      int port;
      Set<String> activeServers;
      Set<String> ineligibleServers;

      port = activeNodes.iterator().next().getPort();
      activeServers = IPAndPort.copyServerIPsAsMutableSet(activeNodes);
      ineligibleServers = removeIneligibleServers(activeServers, dhtRingCurTargetZK, instanceExclusionZK);
      if (!forceInclusionOfUnsafeExcludedServers) {
      for (String ineligibleServer : ineligibleServers) {
        IPAndPort ineligibleNode;

        ineligibleNode = new IPAndPort(ineligibleServer, port);
        activeNodes.remove(ineligibleNode);
      }
      } else {
        Log.warning("Ignoring server eligibility");
      }
    }
  }

  public static Set<String> removeIneligibleServers(Set<String> servers, DHTRingCurTargetZK _dhtRingCurTargetZK,
      InstanceExclusionZK _instanceExclusionZK) throws KeeperException {
    Stat stat;
    long curRingMzxid;
    Set<String> ineligibleServers;
    Map<String, Long> esStarts;

    stat = new Stat();
    _dhtRingCurTargetZK.getCurRingAndVersionPair(stat);
    curRingMzxid = stat.getMzxid();
    ineligibleServers = new HashSet<>();

    esStarts = _instanceExclusionZK.getStartOfCurrentWorrisome(servers);

    for (String server : servers) {
      long startOfCurrentExclusion;
      long startOfCurrentExclusionMzxid;

      startOfCurrentExclusion = esStarts.get(server);
      if (startOfCurrentExclusion < 0) {
        Log.info("No startOfCurrentExclusion for ", server);
      } else {
        startOfCurrentExclusionMzxid = _instanceExclusionZK.getVersionMzxid(startOfCurrentExclusion);
        if (startOfCurrentExclusionMzxid > curRingMzxid) {
          Log.warningf("Ineligible: %s %d > %d", server, startOfCurrentExclusionMzxid, curRingMzxid);
          ineligibleServers.add(server);
        }
      }
    }
    servers.removeAll(ineligibleServers);
    return ineligibleServers;
  }

  /**
   * Disregard accusations from excluded servers
   *
   * @param accuserSuspects
   * @throws KeeperException
   */
  private void removeAccusationsFromExcludedServers(SetMultimap<IPAndPort, IPAndPort> accuserSuspects)
      throws KeeperException {
    ExclusionSet instanceExcludedServers;
    ExclusionSet cloudExcludedServers;
    ExclusionSet unionExcludedServers;

    instanceExcludedServers = instanceExclusionZK.readLatestFromZK();
    cloudExcludedServers = cloudExclusionZK.readLatestFromZK();
    unionExcludedServers = ExclusionSet.union(instanceExcludedServers, cloudExcludedServers);
    for (IPAndPort excludedServer : unionExcludedServers.asIPAndPortSet(gc.getClientDHTConfiguration().getPort())) {
      accuserSuspects.removeAll(excludedServer); // remove all accusations from this excluded accuser
    }
  }

  public void check() {
    boolean exclusionSetWritten;

    exclusionSetWritten = false;
    lastCheckMillis = SystemTimeUtil.skSystemTimeSource.absTimeMillis();
    checkMutex.lock();
    try {
      SetMultimap<IPAndPort, IPAndPort> accuserSuspects;
      SetMultimap<IPAndPort, IPAndPort> suspectAccusers;
      Set<IPAndPort> guiltySuspects;
      Set<IPAndPort> newActiveNodes;
      Set<IPAndPort> newlyInactiveNodes;
      Pair<Set<IPAndPort>, SetMultimap<IPAndPort, IPAndPort>> activeServersAndAccuserSuspects;
      Set<IPAndPort> activeServers;
      long updateDeltaMillis;

      updateDeltaMillis = SystemTimeUtil.skSystemTimeSource.absTimeMillis() - lastUpdateMillis;
      if (updateDeltaMillis < minUpdateIntervalMillis) {
        long sleepMillis;

        sleepMillis = Math.min(minUpdateIntervalMillis - updateDeltaMillis, max_minUpdateIntervalMillis);
        Log.warningf("Check rate control sleeping for %d", sleepMillis);
        ThreadUtil.sleep(sleepMillis);
        Log.warningf("Awake");
      }

      guiltySuspects = new HashSet<>();

      activeServersAndAccuserSuspects = suspectsZK.readAccuserSuspectsFromZK();
      activeServers = activeServersAndAccuserSuspects.getV1();
      accuserSuspects = activeServersAndAccuserSuspects.getV2();
      removeAccusationsFromExcludedServers(accuserSuspects);
      suspectAccusers = CollectionUtil.transposeSetMultimap(accuserSuspects);

      // Read the current active nodes from ZK
      newActiveNodes = new HashSet<>(suspectsZK.readActiveNodesFromZK());
      // Now compute newlyInactiveNodes as the set difference of the previously active nodes minus the active nodes
      // in ZK
      newlyInactiveNodes = new HashSet<>(activeNodes);
      newlyInactiveNodes.removeAll(newActiveNodes);
      filterPassiveNodes(newlyInactiveNodes);
      // verify that all newly active nodes are eligible to return
      //verifyEligibility(newlyInactiveNodes);
      if (!forceInclusionOfUnsafeExcludedServers) {
        verifyEligibility(newActiveNodes);
      }
      // Store activeNodes for the next computation
      activeNodes = newActiveNodes;

      Log.warning(String.format("newlyInactiveNodes\t%s", CollectionUtil.toString(newlyInactiveNodes)));

      // First add newlyInactiveNodes to our guiltySuspects set
      // Throttle the inactive marking
      // FUTURE - improve/remove this
            /*
            if (newlyInactiveNodes.size() <= inactiveNodeMarkingThreshold_servers) {
                guiltySuspects.addAll(newlyInactiveNodes);
            } else if ((double)newlyInactiveNodes.size() / (double)activeNodes.size() 
                                                    <= inactiveNodeMarkingThreshold_fraction) {
                guiltySuspects.addAll(newlyInactiveNodes);
            } else {
                Log.warning("Not marking newly inactive nodes "+ newlyInactiveNodes.size());
            }
            */
      guiltySuspects.addAll(newlyInactiveNodes);

      // Now look through the list of suspects for additional nodes to add to guiltySuspects
      Set<IPAndPort>  activeSuspects;

      activeSuspects = new HashSet<>(suspectAccusers.keySet());
      filterPassiveNodes(activeSuspects);
      for (IPAndPort suspect : activeSuspects) {
        Set<IPAndPort> accusers;

        accusers = suspectAccusers.get(suspect);
        // First check to see if this suspect has maintained its ephemeral node
        // in the suspects list. If it has not, then we presume that the loss of
        // the ephemeral node + the suspicion by another is sufficient to
        // prove that this node is bad.
        if (!activeServers.contains(suspect)) {
          Log.warning(String.format("Guilty 1: %s (at least one accuser, and no ephemeral node)", suspect));
          guiltySuspects.add(suspect);
        } else if (accusers.contains(suspect)) {
          Log.warning(String.format("Guilty 4: %s (self-accusation)", suspect));
          guiltySuspects.add(suspect);
        } else {
          Log.warning(
              String.format("suspectAccusers contains suspect %s, maps to %s", suspect, suspectAccusers.get(suspect)));
          if (accusers.size() >= guiltThreshold) {
            Log.warning(String.format("Guilty 2 %s (accusers.size() %d >= guiltThreshold %d)", suspect, accusers.size(),
                guiltThreshold));
            guiltySuspects.add(suspect);
          } else {
            Log.warning(
                String.format("Not guilty 3 %s (accusers.size() %d < guiltThreshold %d)", suspect, accusers.size(),
                    guiltThreshold));
          }
        }
      }

      newActiveNodes.removeAll(guiltySuspects);

      if (!disableAddition) {
        removeFromConvictionTimes(newActiveNodes);
      }

      // conviction warning check
      if (convictionWarningThresholds != null) {
        if (guiltySuspects.size() > convictionWarningThresholds.getTotalGuiltyServers()) {
          Log.severe("guiltySuspects.size() > convictionWarningThresholds.getTotalGuiltyServers()");
          Log.severef("%d > %d", guiltySuspects.size(), convictionWarningThresholds.getTotalGuiltyServers());
        } else {
          int convictionsWithinOneHour;

          convictionsWithinOneHour = getConvictionsWithinTimeWindow(oneHourMillis);
          if (convictionsWithinOneHour > convictionWarningThresholds.getGuiltyServersPerHour()) {
            Log.severe("convictionsWithinOneHour > convictionWarningThresholds.getGuiltyServersPerHour()");
            Log.severef("%d > %d", convictionsWithinOneHour, convictionWarningThresholds.getGuiltyServersPerHour());
          }
        }
      }

      // conviction limit check
      if (guiltySuspects.size() > convictionLimits.getTotalGuiltyServers()) {
        Log.severe("guiltySuspects.size() > convictionLimits.getTotalGuiltyServers()");
        Log.severef("%d > %d", guiltySuspects.size(), convictionLimits.getTotalGuiltyServers());
      } else {
        int convictionsWithinOneHour;

        convictionsWithinOneHour = getConvictionsWithinTimeWindow(oneHourMillis);
        if (convictionsWithinOneHour > convictionLimits.getGuiltyServersPerHour()) {
          Log.severe("convictionsWithinOneHour > convictionLimits.getGuiltyServersPerHour()");
          Log.severef("%d > %d", convictionsWithinOneHour, convictionLimits.getGuiltyServersPerHour());
        } else {
          boolean updated;

          addToConvictionTimes(guiltySuspects, SystemTimeUtil.skSystemTimeSource.absTimeMillis());
          if (doctor != null) {
            doctor.admitPatients(guiltySuspects);
            if (!disableAddition) {
              doctor.releasePatients(newActiveNodes);
            }
          }

          // FIXME - We need to check if the newActiveNodes are either in the current or target ring. If so, we
          // remove them so that they are not activated.
          // We can't allow them to become active as this could result in data loss.

          updated = updateInstanceExclusionSet(guiltySuspects, disableAddition ? ImmutableSet.of() : newActiveNodes);
          if (updated) {
            lastUpdateMillis = SystemTimeUtil.skSystemTimeSource.absTimeMillis();
          }
        }
      }
    } catch (Exception e) {
      Log.logErrorWarning(e);
    } finally {
      checkMutex.unlock();
    }
  }

  private void filterPassiveNodes(Set<IPAndPort> nodes) {
    Set<IPAndPort> passiveNodes;

    passiveNodes = new HashSet<>();
    for (IPAndPort node : nodes) {
      if (!activeNodesInMap.contains(node)) {
        Log.warning("Ignoring passive node: " + node);
        passiveNodes.add(node);
      }
    }
    nodes.removeAll(passiveNodes);
  }

  private void removeFromConvictionTimes(Set<IPAndPort> healthServers) {
    for (IPAndPort server : healthServers) {
      convictionTimes.remove(server);
    }
  }

  private void addToConvictionTimes(Set<IPAndPort> guiltySuspects, long absTimeMillis) {
    for (IPAndPort suspect : guiltySuspects) {
      if (!convictionTimes.containsKey(suspect)) {
        convictionTimes.put(suspect, absTimeMillis);
      }
    }
  }

  private int getConvictionsWithinTimeWindow(long relTimeMillis) {
    long windowStart;
    int totalConvictions;

    windowStart = SystemTimeUtil.skSystemTimeSource.absTimeMillis() - relTimeMillis;
    totalConvictions = 0;
    for (long convictionTimeMillis : convictionTimes.values()) {
      if (convictionTimeMillis > windowStart) {
        ++totalConvictions;
      }
    }
    return totalConvictions;
  }

  private Set<String> hostStringSet(Set<IPAndPort> s) {
    ImmutableSet.Builder<String> ss;

    ss = ImmutableSet.builder();
    for (IPAndPort ipAndPort : s) {
      ss.add(ipAndPort.getIPAsString());
    }
    return ss.build();
  }

  private boolean updateInstanceExclusionSet(Set<IPAndPort> guiltySuspects, Set<IPAndPort> newActiveNodes) {
    boolean exclusionSetWritten;

    exclusionSetWritten = false;
    try {
      if (instanceExclusionZK != null) {
        ExclusionSet oldExclusionSet;
        ExclusionSet newExclusionSet;

        if (!guiltySuspects.isEmpty()) {
          Log.warning(String.format("Marking as bad"));
          Log.warning(String.format("%s\n", CollectionUtil.toString(hostStringSet(guiltySuspects))));
        } else {
          Log.warning(String.format("No guilty suspects"));
        }
        if (!newActiveNodes.isEmpty()) {
          Log.warning(String.format("Marking as good"));
          Log.warning(String.format("%s\n", CollectionUtil.toString(hostStringSet(newActiveNodes))));
        } else {
          Log.warning(String.format("No newly good nodes"));
        }

        Log.warning(String.format("Latest exclusion set path %s", instanceExclusionZK.getLatestZKPath()));
        if (instanceExclusionZK.getLatestZKPath() != null) {
          oldExclusionSet = instanceExclusionZK.readLatestFromZK();
        } else {
          oldExclusionSet = ExclusionSet.emptyExclusionSet(0);
        }
        newExclusionSet = oldExclusionSet.add(hostStringSet(guiltySuspects)).remove(hostStringSet(newActiveNodes));
        // window of vulnerability here
        // for now we ensure this isn't violated externally
        Log.warning(String.format("Old exclusion set %d %s", oldExclusionSet.size(), oldExclusionSet));
        Log.warning(String.format("New exclusion set %d %s", newExclusionSet.size(), newExclusionSet));
        if (!newExclusionSet.equals(oldExclusionSet)) {
          int minReplicaSet;

          minReplicaSet = ringIntegrityCheck.checkIntegrity(newExclusionSet, false);
          Log.warningf("minReplicaSet: %d", minReplicaSet);
          if (minReplicaSet > 0) {
            Log.warning(String.format("Writing exclusion set"));

            if (!ringHealthNodeCheckedForInitialization) {
              RingHealthZK ringHealthZK;

              ringHealthNodeCheckedForInitialization = true;
              ringHealthZK = new RingHealthZK(mc, dhtRingCurTargetZK.getCurRingAndVersionPair(new Stat()));
              ringHealthZK.writeHealth(RingHealth.Healthy);
            }

            instanceExclusionZK.writeToZK(newExclusionSet);
            exclusionSetWritten = true;
            Log.warning(
                String.format("Latest exclusion set path after write %s", instanceExclusionZK.getLatestZKPath()));
          } else {
            RingHealthZK ringHealthZK;

            Log.severe("Not writing exclusion set as a replica set would be completely excluded");
            List<Set<IPAndPort>> lastExcludedSets;

            ringHealthZK = new RingHealthZK(mc, dhtRingCurTargetZK.getCurRingAndVersionPair(new Stat()));
            ringHealthZK.writeHealth(RingHealth.Unhealthy);
            lastExcludedSets = ringIntegrityCheck.getLastExcludedSets();
            if (lastExcludedSets != null) {
              for (Set<IPAndPort> excludedSet : lastExcludedSets) {
                Log.warningf("%s", CollectionUtil.toString(excludedSet));
              }
            }
          }
        } else {
          Log.warning(String.format("No change in exclusion set"));
          if (!ringHealthNodeCheckedForInitialization) {
            RingHealthZK ringHealthZK;

            ringHealthNodeCheckedForInitialization = true;
            ringHealthZK = new RingHealthZK(mc, dhtRingCurTargetZK.getCurRingAndVersionPair(new Stat()));
            ringHealthZK.writeHealth(RingHealth.Healthy);
          }
        }
      } else {
        Log.warning(String.format("Unable to mark as bad as exclusionZK is null"));
      }
    } catch (Exception e) {
      Log.logErrorWarning(e, "Exception in updateInstanceExclusionSet");
    }
    return exclusionSetWritten;
  }

  public void monitor() {
    waitForDHTMetaUpdate();
    watcher = new ChildrenWatcher(mc, mc.getMetaPaths().getInstanceSuspectsPath(), this, watchIntervalSeconds * 1000);
    running = true;
    while (running) {
      ThreadUtil.sleep(1000);
      if (SystemTimeUtil.skSystemTimeSource.absTimeMillis() - lastCheckMillis > forcedCheckIntervalMillis) {
        Log.warning("Forcing check()");
        check();
      }
    }
  }

  private void waitForDHTMetaUpdate() {
    Log.warning("in waitForDHTMetaUpdate");
    synchronized (this) {
      while (instanceExclusionZK == null) {
        try {
          this.wait();
        } catch (InterruptedException e) {
        }
      }
    }
    Log.warning("out waitForDHTMetaUpdate");
  }

  //////////////////////////

  class DoctorRunner implements Runnable {
    private boolean running;

    DoctorRunner() {
    }

    void start() {
      synchronized (this) {
        if (!running) {
          running = true;
          Log.warning("Starting " + doctorThreadName);
          new Thread(this, doctorThreadName).start();
        }
      }
    }

    void stop() {
      Log.warning("Stopping " + doctorThreadName);
      running = false;
    }

    @Override
    public void run() {
      try {
        while (running) {
          if (dhtRingCurTargetZK == null) {
            Log.warning("Doctor skipping rounds due to no dhtRingCurTargetZK");
          } else {
            if (dhtRingCurTargetZK.curAndTargetRingsMatch()) {
              Log.warning("Doctor will make rounds as cur and target rings match");
              Log.warning("Doctor acquiring checkMutex");
              checkMutex.lock();
              try {
                Log.warning("Doctor is making rounds");
                doctor.makeRounds();
                Log.warning("Doctor is done making rounds");
              } finally {
                checkMutex.unlock();
                Log.warning("Doctor releasing checkMutex");
              }
            } else {
              Log.warning("Doctor is skipping rounds as cur and target rings do not match (convergence is ongoing)");
            }
          }
          Log.warning("Doctor is sleeping");
          ThreadUtil.sleepSeconds(doctorRoundIntervalSeconds);
          Log.warning("Doctor is awake");
        }
      } catch (Exception e) {
        Log.logErrorWarning(e);
      } finally {
        Log.warning("Exiting " + doctorThreadName);
      }
    }
  }

  //////////////////////////

  public static HealthMonitorOptions parseHealthMonitorOptions(String[] args) {
    CmdLineParser parser;
    HealthMonitorOptions options;

    options = new HealthMonitorOptions();
    parser = new CmdLineParser(options);
    try {
      parser.parseArgument(args);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
    return options;
  }

  public static void run(HealthMonitorOptions options) {
    try {
      HealthMonitor healthMonitor;
      SKGridConfiguration gc;
      ConvictionLimits convictionLimits;
      ConvictionLimits convictionWarningThresholds;

      gc = SKGridConfiguration.parseFile(options.gridConfig);
      convictionLimits = ConvictionLimits.parse(options.convictionLimits);
      if (options.convictionWarningThresholds != null) {
        convictionWarningThresholds = ConvictionLimits.parse(options.convictionWarningThresholds);
      } else {
        convictionWarningThresholds = null;
      }
      healthMonitor = new HealthMonitor(gc, gc.getClientDHTConfiguration().getZKConfig(),
          options.watchIntervalSeconds, options.guiltThreshold, options.doctorRoundIntervalSeconds,
          options.forceInclusionOfUnsafeExcludedServers, convictionLimits, convictionWarningThresholds,
          options.doctorNodeStartupTimeoutSeconds, options.disableAddition, options.minUpdateIntervalSeconds * 1000);
      healthMonitor.monitor();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static void main(String[] args) {
    HealthMonitorOptions options;
    SKGridConfiguration gc;

    options = parseHealthMonitorOptions(args);
    try {
      gc = SKGridConfiguration.parseFile(options.gridConfig);
      LogStreamConfig.configureLogStreams(gc, logFileName);
      run(options);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
