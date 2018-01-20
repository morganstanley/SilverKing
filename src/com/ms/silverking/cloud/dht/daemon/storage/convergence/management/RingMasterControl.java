package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import java.rmi.Remote;
import java.rmi.RemoteException;

import com.ms.silverking.cloud.dht.daemon.storage.convergence.management.CentralConvergenceController.SyncTargets;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.id.UUIDBase;

public interface RingMasterControl extends Remote {
	public static final String	baseRegistryName = RingMasterControl.class.getName();
	
	public static String getRegistryName(String dhtName) throws RemoteException {
		return RingMasterControl.class.getName() +"."+ dhtName;
	}
	
	public static String getRegistryName(SKGridConfiguration gc) throws RemoteException {
		return getRegistryName(gc.getClientDHTConfiguration().getName());
	}
	
	public void setMode(Mode mode) throws RemoteException;
	public UUIDBase setTarget(Triple<String,Long,Long> target) throws RemoteException;
	public UUIDBase syncData(Triple<String,Long,Long> source, Triple<String,Long,Long> target, SyncTargets syncTargets) throws RemoteException;
	public String getDHTConfiguration() throws RemoteException;
	public Mode getMode() throws RemoteException;
	public void stop(UUIDBase uuid) throws RemoteException;
	public void reap() throws RemoteException;
	public void waitForCompletion(UUIDBase uuid) throws RemoteException;
	public RequestStatus getStatus(UUIDBase uuid) throws RemoteException;
	public UUIDBase getCurrentConvergenceID() throws RemoteException;
	public UUIDBase recoverData() throws RemoteException;
	public void requestChecksumTree(Triple<Long,Long,Long> nsAndRegion, Triple<String, Long, Long> source, Triple<String, Long, Long> target, String owner) throws RemoteException;
}
