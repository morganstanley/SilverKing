package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.daemon.ReplicaNaiveIPPrioritizer;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.dht.management.MetaUtil;
import com.ms.silverking.cloud.dht.management.MetaUtilOptions;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.toporing.InstantiatedRingTree;
import com.ms.silverking.cloud.toporing.ResolvedReplicaMap;
import com.ms.silverking.cloud.toporing.RingEntry;
import com.ms.silverking.cloud.toporing.SingleRingZK;
import com.ms.silverking.cloud.toporing.meta.MetaClient;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.net.IPAndPort;

public class RingIntegrityCheck {
	private final SKGridConfiguration	gc;
	private final PrintStream			out;
	private final MetaUtil				metaUtil;
	private final com.ms.silverking.cloud.dht.meta.MetaClient	dhtMC;
	private final String				ringParentName; // FUTURE - remove to make functional
	
	public RingIntegrityCheck(SKGridConfiguration gc) throws IOException, KeeperException, ClientException {
		this.gc = gc;
		this.out = System.out;
		
		metaUtil = new MetaUtil(gc.getClientDHTConfiguration().getName(), gc.getClientDHTConfiguration().getZKConfig(), MetaUtilOptions.dhtVersionUnspecified);
		dhtMC = metaUtil.getDHTMC();
		ringParentName = metaUtil.getRingConfiguration().getRingParentName();
	}
	
	public void checkIntegrity(Triple<String, Long, Long> ring, ExclusionSet exclusionSet) throws IOException, KeeperException, ClientException {
		ResolvedReplicaMap	rMap;
		int					setsExcluded;
		
		System.out.printf("exclusionSet %s\n", exclusionSet);
		setsExcluded = 0;
		rMap = readReplicaMap(ring);
		for (Set<IPAndPort> replicaSet : rMap.getReplicaSets()) {
			boolean	allExcluded;
			int		numExcluded;
			
			numExcluded = 0;
			allExcluded = true;
			//System.out.printf("replicaSet %s\n", replicaSet);
			for (IPAndPort member : replicaSet) {
				//System.out.printf("member %s %s\n", member.getIPAsString(), exclusionSet.contains(member.getIPAsString()));
				if (!exclusionSet.contains(member.getIPAsString())) {
					allExcluded = false;
				} else {
					++numExcluded;
				}
			}
			System.out.printf("%s\t%d\n", replicaSet, numExcluded);
			if (allExcluded) {
				++setsExcluded;
				System.out.printf("Set is excluded: %s\n", replicaSet);
			}
		}
		System.out.printf("setsExcluded: %d\n", setsExcluded);
	}
	
	private Map<IPAndPort,Long> getAllocationMap(ResolvedReplicaMap map) {
		Map<IPAndPort,Long>	am;
		
		am = new HashMap<>();
		for (RingEntry entry : map.getEntries()) {
			for (IPAndPort replica : entry.getOwnersIPList(OwnerQueryMode.Primary)) {
				Long	curAllocation;
				
				curAllocation = am.get(replica);
				if (curAllocation == null) {
					curAllocation = new Long(0);
				}
				curAllocation += entry.getRegion().getSize();
				am.put(replica, curAllocation);
			}
		}
		return am;
	}
	
	private ResolvedReplicaMap readReplicaMap(Triple<String,Long,Long> ring) throws IOException, KeeperException {
		return readTree(ring).getResolvedMap(ringParentName, new ReplicaNaiveIPPrioritizer());
	}
	
	private InstantiatedRingTree readTree(Triple<String,Long,Long> ring) throws IOException, KeeperException {
		MetaClient	ringMC;
		long	ringConfigVersion;
		long	configInstanceVersion;
		InstantiatedRingTree	ringTree;
		
		ringConfigVersion = ring.getTail().getV1();
		configInstanceVersion = ring.getTail().getV2();
		
		ringMC = metaUtil.getRingMC();
		
		ringTree = SingleRingZK.readTree(ringMC, ringConfigVersion, configInstanceVersion);
		return ringTree;
	}
	
	private static Triple<String,Long,Long> getRingAndVersionPair(String ringNameAndVersionPair) {
		String[]	s;
		
		s = ringNameAndVersionPair.split(",");
		return new Triple<>(s[0], Long.parseLong(s[1]), Long.parseLong(s[2]));
	}	
	
	public static void main(String[] args) {
		if (args.length != 3) {
			System.err.println("args: <gridConfig> <ring> <exclusionSet>");
		} else {
			try {
				SKGridConfiguration	gc;
				RingIntegrityCheck			ringInfo;
				ExclusionSet				exclusionSet;
				
				gc = SKGridConfiguration.parseFile(args[0]);
				ringInfo = new RingIntegrityCheck(gc);
				if (args[2].indexOf(',') >= 0 || IPAddrUtil.isValidIP(args[2])) {
					exclusionSet = ExclusionSet.parse(args[2]);
				} else {
					exclusionSet = ExclusionSet.parse(new File(args[2]));
				}
				ringInfo.checkIntegrity(getRingAndVersionPair(args[1]), exclusionSet);
				System.exit(0);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
