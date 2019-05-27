package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ConvergencePoint;
import com.ms.silverking.cloud.dht.meta.DHTMetaReader;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.toporing.ResolvedReplicaMap;
import com.ms.silverking.cloud.toporing.RingEntry;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;

/**
 * Search all replicas for data. Currently, this will only function on relatively small-scale instances.
 */
public class RecoverDataController extends ConvergenceControllerBase implements RequestController {
	private final Set<IPAndPort>	targetReplicas;
	private final ResolvedReplicaMap	targetMap;
	
	private static final boolean	verbose = true || ConvergenceControllerBase.verbose;
	private static final boolean	debug = true || ConvergenceControllerBase.debug;
	
	private static final boolean	serializeNamespaces = false;
	
	public RecoverDataController(UUIDBase uuid, DHTMetaReader dhtMetaReader, ConvergencePoint targetCP, 
										ExclusionSet exclusionSet, MessageGroupBase	mgBase) throws KeeperException, IOException {	
		super(uuid, dhtMetaReader, targetCP, exclusionSet, mgBase);
		targetMap = getResolvedReplicaMap(targetRing, targetRingConfig);
		targetReplicas = targetMap.allReplicas();
	}
	
    //////////////////////////////////////////////////////////////////
	
    private void recoverRegion(long ns, RingEntry targetEntry, Action nsSync, List<ReplicaSyncRequest> srList) throws ConvergenceException {
    	Log.warningAsyncf("recoverRegion %x %s", ns, targetEntry);
    	
        Log.warningAsyncf("%x target %s\n", ns, targetEntry.getRegion());
		for (IPAndPort newOwner : targetEntry.getOwnersIPList(OwnerQueryMode.Primary)) {
			Action	prev;
			
			prev = nsSync;
    		for (IPAndPort source : targetReplicas) {
        		prev = syncReplica(ns, targetEntry.getRegion(), newOwner.port(dhtConfig.getPort()), source.port(dhtConfig.getPort()), prev, srList);
    		}
		}
    	Log.warningAsyncf("Done recoverRegion %x %s", ns, targetEntry);
    }    
    
    private Action recoverNamespace(long ns, Action upstreamDependency) throws ConvergenceException {
    	Set<RingEntry>	entries;
    	SynchronizationPoint	syncPoint;    	
    	List<ReplicaSyncRequest> srList;
    	
    	Log.warningAsyncf("Recovering %x", ns);
    	srList = new ArrayList<>();
    	entries = targetReplicaMap.getEntries();
    	for (RingEntry entry : entries) {
    		recoverRegion(ns, entry, upstreamDependency, srList);
    	}
    	
    	syncPoint = SynchronizationPoint.of(Long.toHexString(ns), srList.toArray(new Action[0]));
    	// Note: downstream computed from upstream later
    	if (serializeNamespaces) {
    		syncController.addAction(syncPoint);
    	} else {
    		syncController.addCompleteAction(syncPoint);
    	}
    	
    	Log.warningAsyncf("Done synchronizing %x", ns);
    	return syncPoint;
    }
    
    public void recoverAll(Set<Long> namespaces) throws ConvergenceException {
    	Action prevDependency;
    	
    	Log.warningAsync("Recovering namespaces");
    	prevDependency = null;
    	for (long ns : namespaces) {
    		prevDependency = recoverNamespace(ns, prevDependency);
    	}
    	Log.warningAsync("Done recovering namespaces");
    	
    	syncController.freeze();
    	
    	Log.warningAsync(" *** Sending requests");
    	syncController.waitForCompletion(1, TimeUnit.DAYS); // FUTURE - improve this from a failsafe to a real limit
    	Log.warningAsync(" *** Requests complete");
    }
    
    public void recover() throws ConvergenceException {
    	boolean	succeeded;
    	
    	succeeded = false;
    	try {
	    	Set<Long>	namespaces;
	    	
	    	Log.warningAsync("Starting recovery ", targetRing.getRingIDAndVersionPair());
	    	namespaces = getAllNamespaces();
	    	recoverAll(namespaces);
	    	Log.warningAsync("Recovery complete", targetRing.getRingIDAndVersionPair());
	    	succeeded = true;
    	} catch (ConvergenceException ce) {
    		Log.logErrorWarning(ce, "Recovery failed"+ targetRing.getRingIDAndVersionPair());
    		throw ce;
    	} finally {
    		setComplete(succeeded);
    	}
    }
    
	///////////////////////////////////////////////////
	
	@Override
	public RequestStatus getStatus(UUIDBase uuid) {
		ensureUUIDMatches(uuid);
		if (syncController != null) {
			return new SimpleRequestStatus(getRequestState(), "Recovery:"+ syncController.getStatus().toString());
		} else {
			return new SimpleRequestStatus(getRequestState(), "<init>");
		}
	}
}
