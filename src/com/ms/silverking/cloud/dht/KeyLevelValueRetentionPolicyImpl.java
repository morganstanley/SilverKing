package com.ms.silverking.cloud.dht;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;

public abstract class KeyLevelValueRetentionPolicyImpl<S extends ValueRetentionState>
    extends ValueRetentionPolicyImpl<S> {
  static {
    ObjectDefParser2.addParserWithExclusions(KeyLevelValueRetentionPolicyImpl.class, null, FieldsRequirement.ALLOW_INCOMPLETE,
        null);
  }

  public abstract boolean considersStoredLength();

  public abstract boolean considersInvalidations();

  public abstract boolean retains(DHTKey key, long version, long creationTimeNanos, boolean invalidated, S valueRetentionState,
      long curTimeNanos, int storedLength);
}
