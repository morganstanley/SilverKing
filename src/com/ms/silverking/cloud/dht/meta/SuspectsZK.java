package com.ms.silverking.cloud.dht.meta;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;

/**
 * Used to track nodes that are suspected of being bad. Each node in an instance may accuse other
 * nodes of being suspicious. For each node, a list of suspects is maintained. Thus the
 * mapping in zookeeper is accuser->list of suspects
 */
public class SuspectsZK extends MetaToolModuleBase<SetMultimap<String, String>, MetaPaths> {
  private static final char suspectsDelimiter = ',';
  private static final String suspectsDelimiterString = "" + suspectsDelimiter;

  private static final String emptySetDef = "<empty>";

  private static final boolean debug = false;

  public SuspectsZK(MetaClient mc) throws KeeperException {
    super(mc, mc.getMetaPaths().getInstanceSuspectsPath());
  }

  @Override
  public SetMultimap<String, String> readFromFile(File file, long version) throws IOException {
    throw new RuntimeException("readFromFile not implemented");
  }

  @Override
  public SetMultimap<String, String> readFromZK(long version, MetaToolOptions options) throws KeeperException {
    return _readAccuserSuspectsFromZKAsString();
  }

  @Override
  public void writeToFile(File file, SetMultimap<String, String> instance) throws IOException {
    throw new RuntimeException("writeToFile not implemented");
  }

  @Override
  public String writeToZK(SetMultimap<String, String> suspectAccusersMap, MetaToolOptions options)
      throws IOException, KeeperException {
    throw new RuntimeException("writeToZK not implemented");
  }

  /**
   * Read accuser->suspects map from zookeeper as strings.
   *
   * @return accuser->suspects map
   * @throws KeeperException
   */
  private SetMultimap<String, String> _readAccuserSuspectsFromZKAsString() throws KeeperException {
    String basePath;
    List<String> accusers;
    SetMultimap<String, String> accuserSuspectsMap;
    ZooKeeperExtended _zk;

    basePath = mc.getMetaPaths().getInstanceSuspectsPath();
    _zk = mc.getZooKeeper();
    accusers = _zk.getChildren(basePath);
    accuserSuspectsMap = HashMultimap.create();
    for (String accuser : accusers) {
      String suspectsDef;

      suspectsDef = _zk.getString(basePath + "/" + accuser.toString());
      if (!suspectsDef.equals(emptySetDef)) {
        suspectsDef.trim();
        suspectsDef = suspectsDef.substring(1, suspectsDef.length() - 1);
        for (String suspect : suspectsDef.split(suspectsDelimiterString)) {
          accuserSuspectsMap.put(accuser, suspect);
        }
      }
    }
    return accuserSuspectsMap;
  }

  /**
   * Read accuser->suspects map from zookeeper.
   *
   * @return accuser->suspects map
   * @throws KeeperException
   */
  public Pair<Set<IPAndPort>, SetMultimap<IPAndPort, IPAndPort>> readAccuserSuspectsFromZK() throws KeeperException {
    String basePath;
    List<IPAndPort> accusers;
    SetMultimap<IPAndPort, IPAndPort> accuserSuspectsMap;
    ZooKeeperExtended _zk;

    if (debug) {
      Log.warning("in readAccuserSuspectsFromZK()");
    }
    basePath = mc.getMetaPaths().getInstanceSuspectsPath();
    _zk = mc.getZooKeeper();
    accusers = IPAndPort.list(_zk.getChildren(basePath));
    accuserSuspectsMap = HashMultimap.create();
    for (IPAndPort accuser : accusers) {
      Set<IPAndPort> suspects;

      suspects = _readSuspectsFromZK(_zk, accuser);
      if (suspects != null) {
        accuserSuspectsMap.putAll(accuser, suspects);
      }
    }
    if (debug) {
      Log.warning("accuserSuspectsMap.size()\t", accuserSuspectsMap.size());
      Log.warning("out readAccuserSuspectsFromZK()");
    }
    return new Pair<>(ImmutableSet.copyOf(accusers), accuserSuspectsMap);
  }

  public Set<IPAndPort> readActiveNodesFromZK() throws KeeperException {
    String basePath;
    ZooKeeperExtended _zk;

    basePath = mc.getMetaPaths().getInstanceSuspectsPath();
    _zk = mc.getZooKeeper();
    return ImmutableSet.copyOf(IPAndPort.list(_zk.getChildren(basePath)));
  }

  public void writeSuspectsToZK(IPAndPort accuser, Set<IPAndPort> suspects) throws IOException, KeeperException {
    String basePath;
    ZooKeeperExtended _zk;
    String path;

    basePath = mc.getMetaPaths().getInstanceSuspectsPath();
    _zk = mc.getZooKeeper();
    path = basePath + "/" + accuser;
    if (_zk.exists(path)) {
      _zk.setString(path, CollectionUtil.toString(suspects, suspectsDelimiter));
    } else {
      _zk.createString(path, CollectionUtil.toString(suspects, suspectsDelimiter), CreateMode.EPHEMERAL);
    }
  }

  public void clearAllZK() throws IOException, KeeperException {
    String basePath;
    ZooKeeperExtended _zk;

    basePath = mc.getMetaPaths().getInstanceSuspectsPath();
    _zk = mc.getZooKeeper();
    for (String child : zk.getChildren(basePath)) {
      _zk.delete(basePath + "/" + child);
    }
  }

  public Set<IPAndPort> readSuspectsFromZK(IPAndPort accuser) throws KeeperException {
    return _readSuspectsFromZK(mc.getZooKeeper(), accuser);
  }

  private Set<IPAndPort> _readSuspectsFromZK(ZooKeeperExtended _zk, IPAndPort accuser) {
    Set<IPAndPort> suspects;
    String suspectsDef;
    String basePath;

    suspects = new HashSet<>();
    try {
      basePath = mc.getMetaPaths().getInstanceSuspectsPath();
      suspectsDef = _zk.getString(basePath + "/" + accuser.toString());
      if (debug) {
        Log.warning(accuser + "\t" + suspectsDef);
      }
      if (suspectsDef == null) {
        Log.warningf("_readSuspectsFromZK couldn't read suspects");
      } else {
        if (!suspectsDef.equals(emptySetDef)) {
          suspectsDef.trim();
          suspectsDef = suspectsDef.substring(1, suspectsDef.length() - 1);
          for (String suspect : suspectsDef.split(suspectsDelimiterString)) {
            suspects.add(new IPAndPort(suspect));
          }
        }
      }
      return suspects;
    } catch (KeeperException ke) {
      Log.warning("Unable to read suspects for: ", accuser.toString());
      return null;
    }
  }
}
