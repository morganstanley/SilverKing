package com.ms.silverking.cloud.dht.client;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.daemon.DHTNode;
import com.ms.silverking.cloud.dht.daemon.DHTNodeConfiguration;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.cloud.dht.meta.DHTConfigurationZK;
import com.ms.silverking.cloud.dht.meta.DHTRingCurTargetZK;
import com.ms.silverking.cloud.dht.meta.MetaClient;
import com.ms.silverking.cloud.toporing.StaticRingCreator;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfiguration;
import com.ms.silverking.cloud.zookeeper.LocalZKImpl;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.ParseExceptionAction;

public class EmbeddedSK {
	private static final int	defaultReplication = 1;
	
	private static AtomicBoolean	embeddedExist = new AtomicBoolean();
	private static ConcurrentMap<String,NamedRingConfiguration>	namedRingConfigs = new ConcurrentHashMap<>();
	
	public static final String	skPortProperty = EmbeddedSK.class.getName() +".SKPort";
    public static final int		defaultSKPort = 0;
	private static final int	skPort;
	
	static {
		skPort = PropertiesHelper.systemHelper.getInt(skPortProperty, defaultSKPort, ParseExceptionAction.RethrowParseException);
	}	
	
	
	public static boolean embedded() {
		return embeddedExist.get();
	}
	
	public static NamedRingConfiguration getEmbeddedNamedRingConfiguration(String ringName) {
		return namedRingConfigs.get(ringName);
	}
	
	public static void setEmbeddedNamedRingConfiguration(String ringName, NamedRingConfiguration namedRingConfig) {
		embeddedExist.set(true);
		namedRingConfigs.put(ringName, namedRingConfig);
	}
	
	public static ClientDHTConfiguration createEmbeddedSKInstance(String dhtName, String gridConfigName, String ringName, int replication) {
		try {
			int		zkPort;
			Path	tempDir;
			File	zkDir;
			File	skDir;
			ZooKeeperConfig	zkConfig;
			
			// 0) Create LWT work pools
	        LWTPoolProvider.createDefaultWorkPools();

	        // 1) Start an embedded ZooKeeper
			Log.warning("Creating embedded ZooKeeper");
			try {
				tempDir = Files.createTempDirectory(null);
				zkDir = new File(tempDir.toFile(), "zookeeper");
				zkDir.mkdirs();
				skDir = new File(tempDir.toFile(), "silverking");
				skDir.mkdirs();
			} catch (IOException ioe) {
				throw new RuntimeException(ioe);
			}
			zkPort = LocalZKImpl.startLocalZK(zkDir.getAbsolutePath());
			zkConfig = new ZooKeeperConfig(InetAddress.getLoopbackAddress().getHostAddress() +":"+ zkPort);
			Log.warning("Embedded ZooKeeper running at: "+ zkConfig);
			
			DHTNodeConfiguration.setDataBasePath(skDir.getAbsolutePath() +"/data");
			
			// 2) Create ring in ZK		
			Log.warning("Creating ring");
			StaticRingCreator.createStaticRing(ringName, zkConfig, ImmutableSet.of(IPAddrUtil.localIPString()), replication);
			Log.warning("Created: "+ ringName);
			
			// 3) Create DHT Config in ZK
			DHTConfiguration	dhtConfig;
			MetaClient			dhtMC;
			DHTConfigurationZK	dhtConfigZK;
			ClientDHTConfiguration	clientDHTConfig;
			int					dhtPort;
			
			Log.warning("Creating DHT configuration in ZK");
			if (skPort <= 0) {
				dhtPort = ThreadLocalRandom.current().nextInt(10000, 20000); // FIXME
			} else {
				dhtPort = skPort;
			}
			clientDHTConfig = new ClientDHTConfiguration(dhtName, dhtPort, zkConfig);
			dhtMC = new MetaClient(clientDHTConfig);
			dhtConfigZK = new DHTConfigurationZK(dhtMC);
			dhtConfig = DHTConfiguration.emptyTemplate.ringName(ringName).port(dhtPort).passiveNodeHostGroups("").hostGroupToClassVarsMap(new HashMap<String,String>());
			dhtConfigZK.writeToZK(dhtConfig, null);
			Log.warning("Created DHT configuration in ZK");
			
			// 4) Set cur and target rings
			DHTRingCurTargetZK	curTargetZK;
			
			Log.warning("Setting ring targets");
			curTargetZK = new DHTRingCurTargetZK(dhtMC, dhtConfig);
			curTargetZK.setCurRingAndVersionPair(ringName, 0, 0);
			curTargetZK.setTargetRingAndVersionPair(ringName, 0, 0);
			Log.warning("Ring targets set");
			
			// 4) Start DHTNode
			Log.warning("Starting DHTNode");
			new DHTNode(dhtName, zkConfig, 0, false, false);
			Log.warning("DHTNode started");
			
			// 5) Return the configuration to the caller
			return clientDHTConfig;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static ClientDHTConfiguration createEmbeddedSKInstance(String id, int replication) {
		return createEmbeddedSKInstance("SK."+ id, "GC_SK_"+ id, "ring."+ id, replication);
	}
	
	public static ClientDHTConfiguration createEmbeddedSKInstance(String id) {
		return createEmbeddedSKInstance("SK."+ id, "GC_SK_"+ id, "ring."+ id, defaultReplication);
	}
	
	public static ClientDHTConfiguration createEmbeddedSKInstance(int replication) {
		return createEmbeddedSKInstance(new UUIDBase(false).toString(), replication);
	}
	
	public static ClientDHTConfiguration createEmbeddedSKInstance() {
		return createEmbeddedSKInstance(new UUIDBase(false).toString());
	}
	
	public static void main(String[] args) {
		if (args.length != 0) {
			System.out.println("args: none");
		} else {
			try {
				ClientDHTConfiguration	dhtConfig;
				
				dhtConfig = createEmbeddedSKInstance();
				System.out.printf("DHT Configuration: %s\n", dhtConfig);
				ThreadUtil.sleepForever();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
