package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import com.ms.silverking.cloud.dht.daemon.storage.convergence.management.CentralConvergenceController.SyncTargets;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;

public class RingMasterControlImpl extends UnicastRemoteObject implements RingMasterControl {
	private final DHTRingMaster	rm;
	private final Registry		registry;
	
	private static final long serialVersionUID = -1659250097671179852L;
	
	public static final int	defaultRegistryPort = 2099;
	
	protected RingMasterControlImpl(DHTRingMaster rm, int port) throws RemoteException, AlreadyBoundException {
		super();
		this.rm = rm;
		registry = LocateRegistry.createRegistry(port);		
		registry.rebind(RingMasterControl.getRegistryName(rm.getDHTName()), this);
	}
	
	protected RingMasterControlImpl(DHTRingMaster rm) throws RemoteException, AlreadyBoundException {
		this(rm, defaultRegistryPort);
	}
	
	@Override
	public void setMode(Mode mode) {
		rm.setMode(mode);
	}

	@Override
	public Mode getMode() {
		return rm.getMode();
	}
	
	@Override
	public UUIDBase setTarget(Triple<String, Long, Long> target) {
		Log.warningf("RingMasterControlImpl.setTarget %s", target);
		return rm.setTarget(target);
	}
	
	@Override
	public UUIDBase syncData(Triple<String, Long, Long> source, Triple<String, Long, Long> target, SyncTargets syncTargets) {
		Log.warningf("RingMasterControlImpl.syncData %s", source);
		return rm.syncData(source, target, syncTargets);
	}
	
	@Override
	public UUIDBase recoverData() {
		return rm.recoverData();
	}

	@Override
	public void requestChecksumTree(Triple<Long,Long,Long> nsAndRegion, Triple<String, Long, Long> source, Triple<String, Long, Long> target, String owner) {
		Log.warningf("RingMasterControlImpl.requestChecksumTree %s %s %s %s", nsAndRegion, source, target, owner);
		rm.requestChecksumTree(nsAndRegion, source, target, owner);
	}
	
	@Override
	public String getDHTConfiguration() throws RemoteException {
		return rm.getDHTConfiguration().toString();
	}

	/////////////////////////////////////////////////

	@Override
	public void stop(UUIDBase uuid) {
		rm.stop(uuid);
	}

	@Override
	public void waitForCompletion(UUIDBase uuid) {
		rm.waitForCompletion(uuid);
	}

	@Override
	public RequestStatus getStatus(UUIDBase uuid) {
		return rm.getStatus(uuid);
	}
	
	@Override
	public void reap() {
		rm.reap();
	}

	@Override
	public UUIDBase getCurrentConvergenceID() {
		return rm.getCurrentConvergenceID();
	}
}
