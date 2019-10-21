package com.ms.silverking.cloud.dht.meta;

import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.client.ClientDHTConfigurationProvider;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.NamespaceOptionsMode;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.util.PropertiesHelper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import java.io.IOException;


/**
 * NamespaceOptionsModeResolver is used to control NamespaceOptionsMode used by SK instance
 *
 * The NamespaceOptionsMode is designed to loosely detached as a sub-node of the latest dht config node (i.e. the dhtConfig in use for current SK instance)
 * So it has backward compatibility:
 *      It won't break the existing dhtConfig format, but allow us to add extensive fields used to enrich current dhtConfig
 *      Old version silverking will simply ignore the dhtConfig extension in this detached node
 */
public class NamespaceOptionsModeResolver{

    public static final NamespaceOptionsMode defaultNamespaceOptionsMode = NamespaceOptionsMode.valueOf(
            PropertiesHelper.envHelper.getString(DHTConstants.defaultNamespaceOptionsModeEnv, NamespaceOptionsMode.NSP.name()));

    private MetaClient dhtMc;

    public NamespaceOptionsModeResolver(String dhtName, ZooKeeperConfig zkConfig) throws IOException, KeeperException {
        this(new MetaClient(dhtName, zkConfig));
    }

    public NamespaceOptionsModeResolver(NamedDHTConfiguration dhtConfig, ZooKeeperConfig zkConfig, Watcher watcher) throws IOException, KeeperException {
        this(new MetaClient(dhtConfig, zkConfig, watcher));
    }

    public NamespaceOptionsModeResolver(NamedDHTConfiguration dhtConfig, ZooKeeperConfig zkConfig) throws IOException, KeeperException {
        this(new MetaClient(dhtConfig, zkConfig));
    }

    public NamespaceOptionsModeResolver(ClientDHTConfiguration clientDHTConfig) throws IOException, KeeperException {
        this(new MetaClient(clientDHTConfig));
    }

    public NamespaceOptionsModeResolver(ClientDHTConfigurationProvider dhtConfigProvider) throws IOException, KeeperException {
        this(dhtConfigProvider.getClientDHTConfiguration());
    }

    public NamespaceOptionsModeResolver(SKGridConfiguration skGridConfig) throws IOException, KeeperException {
        this(new MetaClient(skGridConfig));
    }

    public NamespaceOptionsModeResolver(MetaClient dhtMc) {
        this.dhtMc = dhtMc;
    }


    private  static final String nsOptionsNodeName = "nsOptionsMode";

    private String resolveNamespaceOptionsModePath(ZooKeeperExtended zk) throws KeeperException {
        return resolveNamespaceOptionsModePath(dhtMc.getLatestDHTConfigurationZkPath(zk));
    }

    private String resolveNamespaceOptionsModePath(String dhtConfigPathInUse) {
        return dhtConfigPathInUse + "/" + nsOptionsNodeName;
    }

    public NamespaceOptionsMode getNamespaceOptionsMode() throws KeeperException {
        ZooKeeperExtended   zk;
        String              nsOptionsModeNodePath;
        String              modeName;

        zk = dhtMc.getZooKeeper();
        nsOptionsModeNodePath = resolveNamespaceOptionsModePath(zk);
        if (zk.exists(nsOptionsModeNodePath)) {
            modeName = zk.getString(nsOptionsModeNodePath);
            return NamespaceOptionsMode.valueOf(modeName);
        } else {
            return defaultNamespaceOptionsMode;
        }
    }

    public void setNamespaceOptionsMode(NamespaceOptionsMode nsOptionsMode, String dhtConfigPathInUse) throws KeeperException {
        ZooKeeperExtended   zk;
        String              nsOptionsModeNodePath;

        zk = dhtMc.getZooKeeper();
        nsOptionsModeNodePath = resolveNamespaceOptionsModePath(dhtConfigPathInUse);
        if (zk.exists(nsOptionsModeNodePath)) {
            zk.setString(nsOptionsModeNodePath, nsOptionsMode.name());
        } else {
            zk.createString(nsOptionsModeNodePath, nsOptionsMode.name());
        }
    }

    public void setNamespaceOptionsMode(NamespaceOptionsMode nsOptionsMode) throws KeeperException {
        ZooKeeperExtended   zk;
        String              nsOptionsModeNodePath;

        zk = dhtMc.getZooKeeper();
        nsOptionsModeNodePath = resolveNamespaceOptionsModePath(zk);
        if (zk.exists(nsOptionsModeNodePath)) {
            zk.setString(nsOptionsModeNodePath, nsOptionsMode.name());
        } else {
            zk.createString(nsOptionsModeNodePath, nsOptionsMode.name());
        }
    }

    // Just used for having an clean ZooKeeper (the downgrade dose NOT necessarily need this method)
    public void cleanAllNamespaceOptionsModeNodes() throws KeeperException{
        ZooKeeperExtended   zk;
        String              basePath;

        zk = dhtMc.getZooKeeper();
        basePath = dhtMc.getMetaPaths().getInstanceConfigPath();
        for (String child : zk.getChildren(basePath)) {
            String  dhtConfigPath;
            String  nsOptionsModeNodePath;

            dhtConfigPath = basePath + "/" + child;
            nsOptionsModeNodePath = resolveNamespaceOptionsModePath(dhtConfigPath);
            if (zk.exists(nsOptionsModeNodePath)) {
                zk.deleteRecursive(nsOptionsModeNodePath);
            }
        }
    }
}
