package com.ms.silverking.cloud.dht;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;

@OmitGeneration
public abstract class FixedRetentionPolicyImpl extends KeyLevelValueRetentionPolicyImpl<ValueRetentionState.Empty> {
  static {
    ObjectDefParser2.addParserWithExclusions(FixedRetentionPolicyImpl.class, null, FieldsRequirement.ALLOW_INCOMPLETE,
        null);
  }

  public FixedRetentionPolicyImpl() {
  }

  @Override
  public ValueRetentionState.Empty createInitialState() {
    return ValueRetentionState.EMPTY;
  }

  @Override
  public boolean considersStoredLength() {
    return false;
  }

  @Override
  public boolean considersInvalidations() {
    return false;
  }

  @Override
  public int hashCode() {
    return this.getClass().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    return this.getClass() == o.getClass();
  }
}
