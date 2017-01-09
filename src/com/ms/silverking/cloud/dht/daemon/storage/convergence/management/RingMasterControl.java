package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import java.rmi.Remote;
import java.rmi.RemoteException;

import com.ms.silverking.collection.Triple;
import com.ms.silverking.id.UUIDBase;

public interface RingMasterControl extends Remote {
	public static final String	registryName = RingMasterControl.class.getName();
	
	public void setMode(Mode mode) throws RemoteException;
	public UUIDBase setTarget(Triple<String,Long,Long> target) throws RemoteException;
	public void display(String object) throws RemoteException;
	public String getDHTConfiguration() throws RemoteException;
	public Mode getMode() throws RemoteException;
	public void stop(UUIDBase uuid) throws RemoteException;
	public void reap() throws RemoteException;
	public void waitForCompletion(UUIDBase uuid) throws RemoteException;
	public RequestStatus getStatus(UUIDBase uuid) throws RemoteException;
	public UUIDBase getCurrentConvergenceID() throws RemoteException;
}
