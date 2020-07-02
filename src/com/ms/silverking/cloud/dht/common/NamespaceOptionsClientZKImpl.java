package com.ms.silverking.cloud.dht.common;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.client.ClientDHTConfigurationProvider;
import com.ms.silverking.cloud.dht.meta.MetaPaths;
import com.ms.silverking.cloud.meta.MetaClientCore;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.log.Log;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NamespaceOptionsClientZKImpl extends NamespaceOptionsClientBase {
  private static final long nanosPerMilli = 1000000;
  private static final long defaultTimeoutMills = 30 * 1000; // 30 seconds
  private final static String implName = "ZooKeeper";

  private final static String softDeletePlaceholder = "deleted";
  private final static String versionNodeName = "versions";

  private static String getZKBaseVersionPath(String nsZKBasePath) {
    return nsZKBasePath + "/" + versionNodeName;
  }

  private static final Watcher defaultNsPropertiesWatcher = null;  // currently we don't support on-the-fly update,
  // so don't need watcher for now
  private final long relTimeoutMillis;
  private final MetaClientCore metaZK;
  private final ClientDHTConfiguration dhtConfig;

  public NamespaceOptionsClientZKImpl(ClientDHTConfigurationProvider dhtConfigurationProvider, long relTimeoutMillis,
      Watcher watcher) throws IOException, KeeperException {
    super(dhtConfigurationProvider);
    this.relTimeoutMillis = relTimeoutMillis;
    this.dhtConfig = dhtConfigurationProvider.getClientDHTConfiguration();
    this.metaZK = new MetaClientCore(dhtConfig.getZKConfig(), watcher);
  }

  public NamespaceOptionsClientZKImpl(ClientDHTConfigurationProvider dhtConfigurationProvider, Watcher watcher)
      throws IOException, KeeperException {
    this(dhtConfigurationProvider, defaultTimeoutMills, watcher);
  }

  public NamespaceOptionsClientZKImpl(ClientDHTConfigurationProvider dhtConfigurationProvider)
      throws IOException, KeeperException {
    this(dhtConfigurationProvider, defaultTimeoutMills, defaultNsPropertiesWatcher);
  }

  private String getNsZKBasePath(String nsDirName) {
    return MetaPaths.getNsPropertiesBasePath(dhtConfig.getName(), nsDirName);
  }

  private String getNsZKBasePath(long nsContext) {
    return getNsZKBasePath(NamespaceUtil.contextToDirName(nsContext));
  }

  private String resolveVersionPath(long nsContext) {
    return getZKBaseVersionPath(getNsZKBasePath(nsContext));
  }

  private long retrieveNsCreationTime(String versionPath) throws KeeperException {
    ZooKeeperExtended zk;

    zk = metaZK.getZooKeeper();
    return zk.getStat(zk.getLeastVersionPath(versionPath)).getCtime() * nanosPerMilli;
  }

  @Override
  protected long getDefaultRelTimeoutMillis() {
    return relTimeoutMillis;
  }

  // Helper method shared by both put and delete
  private void writeNewVersion(long nsContext, String zkNodeContent) throws KeeperException {
    ZooKeeperExtended zk;
    String versionPath;

    zk = metaZK.getZooKeeper();
    versionPath = resolveVersionPath(nsContext);
    if (!zk.exists(versionPath)) {
      zk.createAllNodes(versionPath);
    }
    zk.createString(versionPath + "/", zkNodeContent, CreateMode.PERSISTENT_SEQUENTIAL);
  }

  @Override
  protected void putNamespaceProperties(long nsContext, NamespaceProperties nsProperties)
      throws NamespacePropertiesPutException {
    try {
      if (nsProperties == null) {
        throw new NamespacePropertiesPutException("null NamespaceProperties is given");
      }
      writeNewVersion(nsContext, nsProperties.toString());
    } catch (KeeperException ke) {
      throw new NamespacePropertiesPutException(ke);
    }
  }

  @Override
  protected void deleteNamespaceProperties(long nsContext) throws NamespacePropertiesDeleteException {
    try {
      writeNewVersion(nsContext, softDeletePlaceholder);
    } catch (KeeperException ke) {
      throw new NamespacePropertiesDeleteException(ke);
    }
  }

  private NamespaceProperties retrieveFullNamespaceProperties(String versionPath)
      throws NamespacePropertiesRetrievalException {
    try {
      ZooKeeperExtended zk;
      String skDef;
      NamespaceProperties nsProperties;

      zk = metaZK.getZooKeeper();
      skDef = zk.getString(zk.getLatestVersionPath(versionPath));

      if (skDef.equals(softDeletePlaceholder)) {
        // Current version is soft-deleted, return null to respect the interface behaviour
        return null;
      }

      nsProperties = NamespaceProperties.parse(skDef);
      if (nsProperties.hasCreationTime()) {
        // Migrated nsProperties will have override creationTime, which inherits from __DHT_Meta__
        Log.warning(
            "Retrieved a nsProperties with overrideCreationTime [" + nsProperties.getCreationTime() + "] for ns: [" + nsProperties.getName() + "]");
        return nsProperties;
      } else {
        // Enrich with zk ctime
        return nsProperties.creationTime(retrieveNsCreationTime(versionPath));
      }
    } catch (KeeperException ke) {
      switch (ke.code()) {
      case NONODE:
        // To respect the interface behaviour: return null if no value
        return null;
      default:
        throw new NamespacePropertiesRetrievalException(ke);
      }
    }
  }

  @Override
  protected NamespaceProperties retrieveFullNamespaceProperties(long nsContext)
      throws NamespacePropertiesRetrievalException {
    return retrieveFullNamespaceProperties(resolveVersionPath(nsContext));
  }

  @Override
  public NamespaceProperties getNsPropertiesForRecovery(File nsDir) throws NamespacePropertiesRetrievalException {
    long nsContext;

    // Same logic as retrieveFullNamespaceProperties, this is just for backward compatibility
    nsContext = NamespaceUtil.nameToContext(nsDir.getName());
    return retrieveFullNamespaceProperties(nsContext);
  }

  @Override
  protected String implementationName() {
    return implName;
  }

  ////===== The APIs below are for admin only (not exposed in NamespaceOptionsClientCS / NamespaceOptionsClientSS)
  public void obliterateAllNsProperties() throws NamespacePropertiesDeleteException {
    try {
      String allNsBasePath;
      ZooKeeperExtended zk;

      allNsBasePath = MetaPaths.getGlobalNsPropertiesBasePath(dhtConfig.getName());
      zk = metaZK.getZooKeeper();
      if (zk.exists(allNsBasePath)) {
        zk.deleteRecursive(allNsBasePath);
      }
    } catch (KeeperException ke) {
      throw new NamespacePropertiesDeleteException(ke);
    }
  }

  public Map<String, NamespaceProperties> getAllNamespaceProperties() throws NamespacePropertiesRetrievalException {
    try {
      Map<String, NamespaceProperties> nsNames = new HashMap<>();
      String allNsBasePath;
      ZooKeeperExtended zk;

      allNsBasePath = MetaPaths.getGlobalNsPropertiesBasePath(dhtConfig.getName());
      zk = metaZK.getZooKeeper();
      if (zk.exists(allNsBasePath)) {
        for (String child : zk.getChildren(allNsBasePath)) {
          NamespaceProperties nsProperties;

          nsProperties = retrieveFullNamespaceProperties(getZKBaseVersionPath(allNsBasePath + "/" + child));
          nsNames.put(child, nsProperties);
        }
      }
      return nsNames;
    } catch (KeeperException ke) {
      throw new NamespacePropertiesRetrievalException(ke);
    }
  }
}
