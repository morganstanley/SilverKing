package com.ms.silverking.cloud.dht.daemon.storage.retention;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.cloud.dht.common.DHTKey;

@OmitGeneration
public abstract class KeyLevelValueRetentionPolicyImpl<S extends ValueRetentionState>
    extends ValueRetentionPolicyImpl<S> {
  public abstract boolean considersStoredLength();

  public abstract boolean considersInvalidations();

  public abstract boolean retains(DHTKey key, long version, long creationTimeNanos, boolean invalidated, S valueRetentionState,
      long curTimeNanos, int storedLength);
}
