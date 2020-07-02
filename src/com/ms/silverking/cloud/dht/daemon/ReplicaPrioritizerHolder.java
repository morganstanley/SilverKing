package com.ms.silverking.cloud.dht.daemon;

import com.ms.silverking.net.IPAndPort;

public class ReplicaPrioritizerHolder {
  private static ReplicaPrioritizer replicaPrioritizer;
  private static final Object replicaPrioritizerLock = new Object();

  public static ReplicaPrioritizer getInstance(IPAndPort nodeID) {
    if (replicaPrioritizer == null) {
      synchronized (replicaPrioritizerLock) {
        if (replicaPrioritizer == null) {
          replicaPrioritizer = new SubnetAwareReplicaPrioritizer(nodeID);
        }
      }
    }
    return replicaPrioritizer;
  }

  public static void setInstance(ReplicaPrioritizer rp) {
    synchronized (replicaPrioritizerLock) {
      replicaPrioritizer = rp;
    }
  }
}