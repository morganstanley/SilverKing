package com.ms.silverking.cloud.dht.daemon.storage;

public interface ReapPolicyState {
  public void fullReapComplete(NamespaceStore nsStore);
}
