package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import java.util.Set;

import com.ms.silverking.cloud.dht.SecondaryTarget;

public interface PutOperationContainer extends OperationContainer {
  public long getVersion();

  public byte[] getUserData();

  public short getCCSS();

  public Set<SecondaryTarget> getSecondaryTargets();

  public void sendInitialResults(PutCommunicator pComm);
}
