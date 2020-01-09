package com.ms.silverking.cloud.dht.util;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.daemon.DHTNodeConfiguration;
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceStore;
import com.ms.silverking.cloud.dht.daemon.storage.ReapOnIdlePolicy;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.cloud.dht.meta.DHTConfigurationZK;
import com.ms.silverking.cloud.dht.meta.DHTRingCurTargetZK;
import com.ms.silverking.cloud.dht.meta.MetaClient;
import com.ms.silverking.cloud.meta.MetaClientCore;
import com.ms.silverking.cloud.meta.WatcherBase;
import com.ms.silverking.cloud.toporing.StaticRingCreator;
import com.ms.silverking.cloud.zookeeper.LocalZKImpl;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.util.PropertiesHelper;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

public abstract class DHTNodeTestUtil {

    private static File zkDir;
    private static int zkPort;
    private static ZooKeeperConfig zkConfig;
    private static Path tempDir;

    @BeforeClass
    public static void setUp() throws IOException {
        System.setProperty(DHTConstants.enablePendingPutsProperty, "false");
        // 0) Create LWT work pools
        LWTPoolProvider.createDefaultWorkPools();
        tempDir = Files.createTempDirectory(null);

        zkDir = new File(tempDir.toFile(), "zookeeper");
        zkDir.mkdirs();
        zkPort = LocalZKImpl.startLocalZK(zkDir.getAbsolutePath());

        zkConfig = new ZooKeeperConfig(InetAddress.getLoopbackAddress().getHostAddress() + ":" + zkPort);
        Log.warning("Embedded ZooKeeper running at: " + zkConfig);
    }

    @AfterClass
    public static void stop() {
        Log.warning("Stopping all global DHTNode processes");
        ZooKeeperExtended.stopProcessRunner();
        WatcherBase.stopProcessRunner();
        MetaClientCore.clearZkMap();
        LWTPoolProvider.stopDefaultWorkPools();
        LocalZKImpl.shutdown();
        System.clearProperty(DHTConstants.enablePendingPutsProperty);
        try {
            FileUtils.deleteDirectory(tempDir.toFile());
        } catch(IOException e) {
            Log.logErrorWarning(e);
        }
    }

    public TestDHTNode getDhtNode(String dhtName, int dhtPort, String ringName, int replication) throws KeeperException, IOException {
            File skDir = new File(tempDir.toFile(), "silverking");
            skDir.mkdirs();
            DHTNodeConfiguration nodeConfig = new DHTNodeConfiguration(skDir.getAbsolutePath() + "/data");

            Log.info("Creating ring");
            StaticRingCreator.createStaticRing(ringName, zkConfig, ImmutableSet.of(IPAddrUtil.localIPString()), replication);
            Log.info("Created: " + ringName);

            DHTConfiguration dhtConfig;
            MetaClient dhtMC;
            DHTConfigurationZK dhtConfigZK;
            ClientDHTConfiguration clientDHTConfig;

            Log.info("Creating DHT configuration in ZK");

            clientDHTConfig = new ClientDHTConfiguration(dhtName, dhtPort, zkConfig);
            dhtMC = new MetaClient(clientDHTConfig);
            dhtConfigZK = new DHTConfigurationZK(dhtMC);
            dhtConfig = DHTConfiguration.emptyTemplate.ringName(ringName).port(dhtPort).passiveNodeHostGroups("").hostGroupToClassVarsMap(new HashMap<String, String>());
            dhtConfigZK.writeToZK(dhtConfig, null);
            Log.info("Created DHT configuration in ZK");

            DHTRingCurTargetZK curTargetZK;

            Log.info("Setting ring targets");
            curTargetZK = new DHTRingCurTargetZK(dhtMC, dhtConfig);
            curTargetZK.setCurRingAndVersionPair(ringName, 0, 0);
            curTargetZK.setTargetRingAndVersionPair(ringName, 0, 0);
            Log.info("Ring targets set");

            Log.info("Starting DHTNode");
            TestDHTNode dhtNode = new TestDHTNode(dhtName, zkConfig, nodeConfig, clientDHTConfig, 0, new ReapOnIdlePolicy());
            Log.info("DHTNode started");
            dhtNode.setClientDHTConfiguration(clientDHTConfig);
            return dhtNode;

    }

    public TestDHTNode getDhtNode(String dhtName, DHTNodeConfiguration nodeConfiguration, ClientDHTConfiguration clientDHTConfig) {
        try {
            Log.info("Starting DHTNode");
            TestDHTNode dhtNode = new TestDHTNode(dhtName, zkConfig, nodeConfiguration, clientDHTConfig,0, new ReapOnIdlePolicy());
            Log.info("DHTNode started");
            dhtNode.setClientDHTConfiguration(clientDHTConfig);
            return dhtNode;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
