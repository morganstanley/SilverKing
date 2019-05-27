package com.ms.silverking.cloud.toporing.meta;

import java.io.IOException;
import java.util.Arrays;

import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;

public class NamedRingConfigurationUtil {
	public enum Op	{GetRingName};
	
	public static NamedRingConfiguration fromGridConfiguration(SKGridConfiguration gc) throws IOException, KeeperException {
	    MetaClient              _mc;
	    long                    version;
	    String					ringName;
	    ZooKeeperConfig			zkConfig;
	    com.ms.silverking.cloud.dht.meta.MetaClient	dhtMC;
	    
	    dhtMC = new com.ms.silverking.cloud.dht.meta.MetaClient(gc);
	    ringName = dhtMC.getDHTConfiguration().getRingName();
	    zkConfig = dhtMC.getZooKeeper().getZKConfig();
	    
	    _mc = new MetaClient(new NamedRingConfiguration(ringName, RingConfiguration.emptyTemplate), zkConfig);
	    
	    // FIXME - version never changes
	    version = _mc.getZooKeeper().getLatestVersion(MetaPaths.getRingConfigPath(ringName));
	    return new NamedRingConfiguration(ringName, new RingConfigurationZK(_mc).readFromZK(version, null));
	}

	private static void doOp(Op op, String[] args) throws IOException, KeeperException {
		switch (op) {
		case GetRingName:
			getRingName(args[0]);
			break;
		default:
			throw new RuntimeException("panic");
		}
	}
	
	private static void getRingName(String gcName) throws IOException, KeeperException {
		SKGridConfiguration	gc;
		NamedRingConfiguration	ringConfig;
		
		gc = SKGridConfiguration.parseFile(gcName);
		ringConfig = fromGridConfiguration(gc);
		System.out.println(ringConfig.getRingName());
	}

	private static void displayUsage() {
		System.out.println("<Op> <args...>");
		System.exit(-1);
	}
	
	public static void main(String[] args) {
		try {
			if (args.length < 1)  {
				displayUsage();
			} else {
				Op	op;
				
				op = Op.valueOf(args[0]);
				if (op == null) {
					displayUsage();
				} else {
					doOp(op, Arrays.copyOfRange(args, 1, args.length));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
