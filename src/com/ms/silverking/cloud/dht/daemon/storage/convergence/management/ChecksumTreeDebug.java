package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.ms.silverking.cloud.dht.common.NamespaceUtil;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ActiveRegionSync;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ChecksumNode;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ChecksumTreeRequest;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ConvergencePoint;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.RingID;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.RingIDAndVersionPair;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.net.MessageGroupConnection;
import com.ms.silverking.cloud.dht.net.MessageGroupReceiver;
import com.ms.silverking.cloud.dht.net.ProtoChecksumTreeMessageGroup;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.net.async.OutgoingData;
import com.ms.silverking.thread.lwt.LWTPoolProvider;

public class ChecksumTreeDebug implements MessageGroupReceiver {
	private final MessageGroupBase	mgBase;
	private ActiveRegionSync	ars;
	
	private static final int	queueLimit = 1000;
	
	static {
		ActiveRegionSync.debug = true;
		Log.initAsyncLogging();
		OutgoingData.setAbsMillisTimeSource(SystemTimeUtil.systemTimeSource);
	}
	
	public ChecksumTreeDebug() throws IOException {
		mgBase = new MessageGroupBase(0, this, SystemTimeUtil.systemTimeSource, null, queueLimit, 1, "");
		mgBase.enable();
	}
	
	private static Pair<Long,Long> getVersionPair(String versionPair) {
		String[]	s;
		
		s = versionPair.split(",");
		return new Pair<>(Long.parseLong(s[0]), Long.parseLong(s[1]));
	}

	@Override
	public void receive(MessageGroup messageGroup, MessageGroupConnection connection) {
		switch (messageGroup.getMessageType()) {
		case RETRIEVE_RESPONSE:
			incomingSyncRetrievalResponse(messageGroup);
			break;
		case CHECKSUM_TREE:
			incomingChecksumTree(messageGroup, connection);
			break;
		default:
			throw new RuntimeException("Unexpected message: "+ messageGroup);	
		}
	}
	
    private void incomingChecksumTree(MessageGroup message, MessageGroupConnection connection) {
        ChecksumNode    remoteTree;
        ConvergencePoint    cp;
        
        cp = ProtoChecksumTreeMessageGroup.getConvergencePoint(message);
        remoteTree = ProtoChecksumTreeMessageGroup.deserialize(message);
		ars.incomingChecksumTree(cp, remoteTree, connection);
    }	
    
    public void incomingSyncRetrievalResponse(MessageGroup message) {
        ActiveRegionSync._incomingSyncRetrievalResponse(message);
    }
	
	public void debug(IPAndPort replica, long namespace, RingRegion region, ConvergencePoint sourceCP, ConvergencePoint targetCP) {
		ars = new ActiveRegionSync(namespace, null, mgBase, new ChecksumTreeRequest(targetCP, sourceCP, region, replica));
		ars.startSync();
		ars.waitForCompletion(1, TimeUnit.MINUTES);
	}
	
	public static void main(String[] args) {
		try {
			if (args.length < 8) {
				System.out.println("args: <gridConfig> <sourceIP> <namespace> <dhtConfigVersion> <ringName> <sourceRing> <targetRing> <ringRegion> [dataVersion]");
			} else {
				ChecksumTreeDebug	ctd;
				SKGridConfiguration	gc;
				IPAndPort			sourceNode;
				long				namespace;
				int					dhtConfigVersion;
				RingID				ringID;
				Pair<Long, Long>	sourceRing;
				Pair<Long, Long>	targetRing;
				ConvergencePoint	sourceCP;
				ConvergencePoint	targetCP;
				RingRegion			ringRegion;
				long				dataVersion;
				
		        LWTPoolProvider.createDefaultWorkPools();
				
				gc = SKGridConfiguration.parseFile(args[0]);
				sourceNode = new IPAndPort(args[1], gc.getClientDHTConfiguration().getPort());
				namespace = NamespaceUtil.nameToLong(args[2]);
				dhtConfigVersion = Integer.parseInt(args[3]);
				ringID = RingID.nameToRingID(args[4]);
				sourceRing = getVersionPair(args[5]);
				targetRing = getVersionPair(args[6]);
				ringRegion = RingRegion.parseZKString(args[7]);
				if (args.length >= 9) {
					dataVersion = Long.parseLong(args[8]);
				} else {
					dataVersion = Long.MAX_VALUE;
				}
				sourceCP = new ConvergencePoint(dhtConfigVersion, new RingIDAndVersionPair(ringID, sourceRing), dataVersion);
				targetCP = new ConvergencePoint(dhtConfigVersion, new RingIDAndVersionPair(ringID, targetRing), dataVersion);
				ctd = new ChecksumTreeDebug();
				ctd.debug(sourceNode, namespace, ringRegion, sourceCP, targetCP);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
