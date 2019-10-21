package com.ms.silverking.cloud.dht.common;

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

public class NamespaceOptionsClientZKImpl extends NamespaceOptionsClientBase {
    private static final long       nanosPerMilli = 1000000;
    private static final long       defaultTimeoutMills = 30 * 1000; // 30 seconds
    private final static String     implName = "ZooKeeper";

    private final static String     softDeletePlaceholder = "deleted";
    private final static String     versionNodeName = "versions";
    private final static String     dataNodeName = "data";

    private final Watcher    nsPropertiesWatcher = null;  // currently we don't support on-the-fly update, so don't need watcher for now
    private final long relTimeoutMillis;
    private final MetaClientCore metaZK;
    private final ClientDHTConfiguration dhtConfig;

    public NamespaceOptionsClientZKImpl(ClientDHTConfigurationProvider dhtConfigurationProvider, long relTimeoutMillis) throws IOException, KeeperException {
        super(dhtConfigurationProvider);
        this.relTimeoutMillis = relTimeoutMillis;
        this.dhtConfig = dhtConfigurationProvider.getClientDHTConfiguration();
        this.metaZK = new MetaClientCore(dhtConfig.getZKConfig(), nsPropertiesWatcher);
    }

    public NamespaceOptionsClientZKImpl(ClientDHTConfigurationProvider dhtConfigurationProvider) throws IOException, KeeperException {
        this(dhtConfigurationProvider, defaultTimeoutMills);
    }

    private String getZKBasePath(long nsContext) {
        String  nsNodeName;

        nsNodeName = NamespaceUtil.contextToDirName(nsContext);
        return MetaPaths.getNsPropertiesBasePath(dhtConfig.getName(), nsNodeName);
}

    private String resolveVersionPath(long nsContext) {
        return getZKBasePath(nsContext) + "/" + versionNodeName;
    }

    private String resolveDataDirPath(long nsContext, String id) {
        return getZKBasePath(nsContext) + "/" + dataNodeName + "/" + id;
    }

    private long retrieveNsCreationTime(String versionPath) throws KeeperException {
        ZooKeeperExtended   zk;

        zk = metaZK.getZooKeeper();
        return zk.getStat(zk.getLeastVersionPath(versionPath)).getCtime() * nanosPerMilli;
    }

    @Override
    protected long getDefaultRelTimeoutMillis() {
        return relTimeoutMillis;
    }

    private void writeNewVersion(long nsContext, String zkNodeContent) throws KeeperException {
        ZooKeeperExtended   zk;
        String              versionPath;

        zk = metaZK.getZooKeeper();
        versionPath = resolveVersionPath(nsContext);
        if (!zk.exists(versionPath)) {
            zk.createAllNodes(versionPath);
        }
        zk.createString(versionPath + "/", zkNodeContent, CreateMode.PERSISTENT_SEQUENTIAL);
    }

    @Override
    protected void putNamespaceProperties(long nsContext, NamespaceProperties nsProperties) throws NamespacePropertiesPutException {
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
    protected void deleteAllNamespaceProperties(long nsContext) throws NamespacePropertiesDeleteException {
        try {
            writeNewVersion(nsContext, softDeletePlaceholder);
        } catch (KeeperException ke) {
            throw new NamespacePropertiesDeleteException(ke);
        }
    }

    @Override
    protected NamespaceProperties retrieveFullNamespaceProperties(long nsContext) throws NamespacePropertiesRetrievalException {
        try {
            String              versionPath;
            ZooKeeperExtended   zk;
            String              skDef;
            NamespaceProperties nsProperties;

            versionPath = resolveVersionPath(nsContext);
            zk = metaZK.getZooKeeper();
            skDef = zk.getString(zk.getLatestVersionPath(versionPath));

            if (skDef.equals(softDeletePlaceholder)) {
                // Current version is soft-deleted, return null to respect the interface behaviour
                return null;
            }

            nsProperties = NamespaceProperties.parse(skDef);
            if (nsProperties.hasCreationTime()) {
                // Migrated nsProperties will have override creationTime, which inherits from __DHT_Meta__
                Log.warning("Retrieved a nsProperties with overrideCreationTime [" + nsProperties.getCreationTime() + "] for ns: [" + nsProperties.getName() + "]" );
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
    public NamespaceProperties getNsPropertiesForRecovery(File nsDir) throws NamespacePropertiesRetrievalException {
        long    nsContext;

        // Same logic as retrieveFullNamespaceProperties, this is just for backward compatibility
        nsContext = NamespaceUtil.nameToContext(nsDir.getName());
        return retrieveFullNamespaceProperties(nsContext);
    }

    @Override
    protected String implementationName() {
        return implName;
    }

    public void registerNamespaceDir(long nsContext, String registerInfo) throws NamespacePropertiesPutException {
        try {
            String dataDirPath;
            ZooKeeperExtended zk;

            dataDirPath = resolveDataDirPath(nsContext, registerInfo);
            zk = metaZK.getZooKeeper();
            if (!zk.exists(dataDirPath)) {
                zk.createAllNodes(dataDirPath);
            }
        } catch (KeeperException ke) {
            throw new NamespacePropertiesPutException(ke);
        }
    }

    public void unregisterNamespaceDir(long nsContext, String registerInfo) throws NamespacePropertiesDeleteException {
        try {
            String dataDirPath;
            ZooKeeperExtended zk;

            dataDirPath = resolveDataDirPath(nsContext, registerInfo);
            zk = metaZK.getZooKeeper();
            if (zk.exists(dataDirPath)) {
                zk.deleteRecursive(dataDirPath);
            }
        } catch (KeeperException ke) {
            throw new NamespacePropertiesDeleteException(ke);
        }
    }

    // For clean all ZK nodes only
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
}
