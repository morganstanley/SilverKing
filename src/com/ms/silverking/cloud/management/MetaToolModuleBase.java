package com.ms.silverking.cloud.management;

import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.meta.MetaClientBase;
import com.ms.silverking.cloud.meta.MetaPathsBase;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.log.Log;

public abstract class MetaToolModuleBase<T,M extends MetaPathsBase> implements MetaToolModule<T> {
    protected final MetaClientBase<M>    mc;
    protected final ZooKeeperExtended zk;
    protected final M     paths;
    protected final String        base;
    
    public MetaToolModuleBase(MetaClientBase<M> mc, String base) throws KeeperException {
        this.mc = mc;
        this.zk = mc.getZooKeeper();
        paths = mc.getMetaPaths();
        mc.ensureMetaPathsExist();
        this.base = base;
        if (base == null) {
            Log.warning("base is null");
        }
    }
    
    public String getBase() {
        return base;
    }
    
    protected String getVersionPath(String name, long version) {
        return ZooKeeperExtended.padVersionPath(base +"/"+ name, version);
    }
    
    protected String getVersionPath(long version) {
        return ZooKeeperExtended.padVersionPath(base, version);
    }    
    
    protected String getVBase(String name, long version) throws KeeperException {
        String  vBase;

        vBase = getVersionPath(name, version);
        mc.ensurePathExists(vBase, false);
        return vBase;
    }
    
    protected String getVBase(long version) throws KeeperException {
        String  vBase;

        vBase = getVersionPath(version);
        mc.ensurePathExists(vBase, false);
        return vBase;
    }
    
    @Override
    public void deleteFromZK(long version) throws KeeperException {
        zk.deleteRecursive(getVersionPath(version));
    }
    
    @Override
    public long getLatestVersion() throws KeeperException {
        return zk.getLatestVersion(base);
    }
}

