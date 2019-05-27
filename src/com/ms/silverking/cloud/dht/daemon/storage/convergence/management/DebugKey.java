package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;

import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.dht.NonExistenceResponse;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.SessionOptions;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.StoredValue;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.serialization.internal.StringMD5KeyCreator;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.daemon.ReplicaNaiveIPPrioritizer;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.dht.management.MetaUtil;
import com.ms.silverking.cloud.dht.management.MetaUtilOptions;
import com.ms.silverking.cloud.dht.net.ForwardingMode;
import com.ms.silverking.cloud.toporing.InstantiatedRingTree;
import com.ms.silverking.cloud.toporing.PrimarySecondaryIPListPair;
import com.ms.silverking.cloud.toporing.ResolvedReplicaMap;
import com.ms.silverking.cloud.toporing.SingleRingZK;
import com.ms.silverking.cloud.toporing.meta.MetaClient;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.net.IPAndPort;

public class DebugKey {
	private final SKGridConfiguration	gc;
	private final StringMD5KeyCreator	keyCreator;
	private final PrintStream			out;
	private final MetaUtil				metaUtil;
	private final com.ms.silverking.cloud.dht.meta.MetaClient	dhtMC;
	private final String				ringParentName; // FUTURE - remove to make functional
	private final DHTClient				dhtClient;
	private final String				namespace;
	
	public DebugKey(SKGridConfiguration gc, String namespace) throws IOException, KeeperException, ClientException {
		this.gc = gc;
		this.namespace = namespace;
		keyCreator = new StringMD5KeyCreator();
		this.out = System.out;
		
		metaUtil = new MetaUtil(gc.getClientDHTConfiguration().getName(), gc.getClientDHTConfiguration().getZKConfig(), MetaUtilOptions.dhtVersionUnspecified);
		dhtMC = metaUtil.getDHTMC();
		ringParentName = metaUtil.getRingConfiguration().getRingParentName();
		
		dhtClient = new DHTClient();
	}
	
	public void debugConvergence(String key, Pair<Long, Long> sourceRing, Pair<Long, Long> targetRing) throws IOException, KeeperException, ClientException {
		String	ringName;
		
		ringName = metaUtil.getDHTConfiguration().getRingName();
		debugConvergence(key, Triple.of(ringName, sourceRing), Triple.of(ringName, targetRing));
	}
	
	public void debugConvergence(String key, Triple<String, Long, Long> sourceRing, Triple<String, Long, Long> targetRing) throws IOException, KeeperException, ClientException {
		ResolvedReplicaMap	sMap; 
		ResolvedReplicaMap	tMap; 
		DHTKey	dhtKey;
		
		dhtKey = keyCreator.createKey(key);
		out.printf("Key:    %s\n", key);
		out.printf("DHTKey: %s\n", KeyUtil.keyToString(dhtKey));
		out.printf("Coord:  %d\n", KeyUtil.keyToCoordinate(dhtKey));
		
		sMap = readReplicaMap(sourceRing);
		tMap = readReplicaMap(targetRing);
		out.printf("Source\n");
		displayKeyInMap(dhtKey, sMap);
		searchForKey(key, dhtKey, sMap);
		out.println();
		out.printf("Target\n");
		displayKeyInMap(dhtKey, tMap);
		searchForKey(key, dhtKey, tMap);
	}
	
	private void searchForKey(String key, DHTKey dhtKey, ResolvedReplicaMap map) throws ClientException {
		PrimarySecondaryIPListPair	psIPLists;
		
		out.printf("%s\n", map.getRegion(dhtKey));
		psIPLists = map.getReplicaListPair(dhtKey);
		for (IPAndPort replica : psIPLists.getPrimaryOwners()) {
			out.printf("P %s\t%s\n", replica, replicaContainsKey(replica, key));
		}
		for (IPAndPort replica : psIPLists.getSecondaryOwners()) {
			out.printf("S %s\t%s\n", replica, replicaContainsKey(replica, key));
		}
	}

	private boolean replicaContainsKey(IPAndPort replica, String key) throws ClientException {
		DHTSession	dhtSession;
		SynchronousNamespacePerspective<String,byte[]>	nsp;
		RetrievalOptions	ro;
		StoredValue<byte[]>	storedValue;
		
		dhtSession = dhtClient.openSession(new SessionOptions(gc, replica.getIPAsString()));
		nsp = dhtSession.openSyncNamespacePerspective(namespace, String.class, byte[].class);
		
		ro = nsp.getNamespace().getOptions().getDefaultGetOptions();
		ro = ro.retrievalType(RetrievalType.META_DATA);
		ro = ro.forwardingMode(ForwardingMode.DO_NOT_FORWARD);
		ro = ro.nonExistenceResponse(NonExistenceResponse.NULL_VALUE);
		
		storedValue = nsp.retrieve(key, ro);
		nsp.close();
		dhtSession.close();
		
		if (storedValue == null) {
			return false;
		} else {
			//out.println(storedValue.getMetaData());
			return true;
		}
	}

	private void displayKeyInMap(DHTKey key, ResolvedReplicaMap sMap) {
		PrimarySecondaryIPListPair	psIPLists;
		
		psIPLists = sMap.getReplicaListPair(key);
		out.printf("%s\n", psIPLists);
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
	
	private static Pair<Long,Long> getVersionPair(String versionPair) {
		String[]	s;
		
		s = versionPair.split(",");
		return new Pair<>(Long.parseLong(s[0]), Long.parseLong(s[1]));
	}	
	
	public static void main(String[] args) {
		if (args.length < 4) {
			System.err.println("args: <gridConfig> <ns> <sourceRing> <destRing> <key...>");
		} else {
			try {
				DebugKey	dk;
				SKGridConfiguration	gc;
				String				namespace;
				Pair<Long, Long>	sourceRing;
				Pair<Long, Long>	targetRing;
				String[]			keys;
				
				gc = SKGridConfiguration.parseFile(args[0]);
				namespace = args[1];
				sourceRing = getVersionPair(args[2]);
				targetRing = getVersionPair(args[3]);
				dk = new DebugKey(gc, namespace);
				keys = Arrays.copyOfRange(args, 4, args.length);
				for (String key : keys) {
					dk.debugConvergence(key, sourceRing, targetRing);
				}
				System.exit(0);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
