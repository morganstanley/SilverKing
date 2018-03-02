package com.ms.silverking.pssh;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;

import com.ms.silverking.cloud.config.HostGroupTable;

public interface SSHMaster extends Remote {
    public String getSSHCmd() throws RemoteException;
    public Map<String,String> getSSHCmdMap() throws RemoteException;
    public HostGroupTable getHostGroups() throws RemoteException;
    public HostAndCommand getHostAndCommand() throws RemoteException;
    public void setHostResult(HostAndCommand hostAndCommand, HostResult result) throws RemoteException;
    public void workerComplete() throws RemoteException;
}
