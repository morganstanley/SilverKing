package com.ms.silverking.cloud.meta.test;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.meta.MetaClientCore;
import com.ms.silverking.cloud.meta.VersionListener;
import com.ms.silverking.cloud.meta.VersionWatcher;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;

public class VersionWatcherTest implements VersionListener {
    private final ZooKeeperExtended zk;
    private final MetaClientCore	mcCore;
    
    public VersionWatcherTest(ZooKeeperExtended zk) throws IOException, KeeperException {
        this.zk = zk;
        mcCore = new MetaClientCore(zk.getZKConfig(), null);
    }
    
    @Override
    public void newVersion(String basePath, long version) {
        System.out.println("newVersion: "+ basePath +"\t"+ version);
    }
    
    public void addWatch(String basePath, long intervalMillis) {
        System.out.println("watching: "+ basePath);
        new VersionWatcher(mcCore, basePath, this, intervalMillis);
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            if (args.length < 3) {
                System.out.println("<zkConfig> <intervalSeconds> <path...>");
            } else {
                ZooKeeperConfig zkConfig;
                long            intervalMillis;
                VersionWatcherTest  vwTest;
                
                zkConfig = new ZooKeeperConfig(args[0]);
                intervalMillis = Integer.parseInt(args[1]) * 1000;
                vwTest = new VersionWatcherTest(new ZooKeeperExtended(zkConfig, 2 * 60 * 1000, null));
                for (int i = 2; i < args.length; i++) {
                    vwTest.addWatch(args[i], intervalMillis);
                }
                Thread.sleep(60 * 60 * 1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
