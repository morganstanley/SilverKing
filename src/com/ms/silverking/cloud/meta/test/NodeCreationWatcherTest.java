package com.ms.silverking.cloud.meta.test;

import com.ms.silverking.cloud.meta.NodeCreationListener;
import com.ms.silverking.cloud.meta.NodeCreationWatcher;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;

public class NodeCreationWatcherTest implements NodeCreationListener {
    private final ZooKeeperExtended zk;
    
    public NodeCreationWatcherTest(ZooKeeperExtended zk) {
        this.zk = zk;
    }
    
    @Override
    public void nodeCreated(String path) {
        System.out.println("nodeCreated: "+ path);
    }
    
    public void addWatch(String path) {
        System.out.println("watching: "+ path);
        new NodeCreationWatcher(zk, path, this);
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            if (args.length < 2) {
                System.out.println("<zkConfig> <path...>");
            } else {
                ZooKeeperConfig zkConfig;
                NodeCreationWatcherTest  wTest;
                
                zkConfig = new ZooKeeperConfig(args[0]);
                wTest = new NodeCreationWatcherTest(new ZooKeeperExtended(zkConfig, 2 * 60 * 1000, null));
                for (int i = 1; i < args.length; i++) {
                    wTest.addWatch(args[i]);
                }
                Thread.sleep(60 * 60 * 1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
