package com.ms.silverking.cloud.dht.daemon;

import com.ms.silverking.cloud.dht.common.DHTKey;

public interface KeyToReplicaResolver {
  public boolean iAmPotentialReplicaFor(DHTKey dhtKey, boolean discountExcludedNodes);
}