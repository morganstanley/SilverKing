package com.ms.silverking.cloud.dht;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.serverside.PutTrigger;
import com.ms.silverking.cloud.dht.serverside.RetrieveTrigger;

public interface ValueRetentionPolicy<T extends ValueRetentionState> {
  public enum ImplementationType {SingleReverseSegmentWalk, RetainAll}

  ;

  public ImplementationType getImplementationType();

  public boolean retains(DHTKey key, long version, long creationTimeNanos, boolean invalidated, T valueRetentionState,
      long curTimeNanos, long storedLength);

  public T createInitialState(PutTrigger putTrigger, RetrieveTrigger retrieveTrigger);

  public boolean considersStoredLength();
}
