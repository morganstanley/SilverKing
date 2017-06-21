package com.ms.silverking.pssh;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;

public interface SSHMaster extends Remote {
    public Map<String,String> getSSHCmdMap() throws RemoteException;
    public HostAndCommand getHostAndCommand() throws RemoteException;
    public void setHostResult(HostAndCommand hostAndCommand, HostResult result) throws RemoteException;
    public void workerComplete() throws RemoteException;
}
