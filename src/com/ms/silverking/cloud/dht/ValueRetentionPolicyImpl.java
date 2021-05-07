package com.ms.silverking.cloud.dht;

import com.ms.silverking.cloud.dht.daemon.storage.NamespaceStore;
import com.ms.silverking.cloud.dht.daemon.storage.NeverRetentionPolicy;
import com.ms.silverking.cloud.dht.daemon.storage.NeverRetentionPolicyImpl;
import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;

public abstract class ValueRetentionPolicyImpl<T extends ValueRetentionState> {
  public enum ImplementationType {SingleReverseSegmentWalk, RetainAll};

  static {
    ObjectDefParser2.addParserWithExclusions(ValueRetentionPolicyImpl.class, null, FieldsRequirement.ALLOW_INCOMPLETE,
        null);
  }

  public abstract ImplementationType getImplementationType();
  public abstract T createInitialState();

  public static ValueRetentionPolicyImpl fromPolicy(ValueRetentionPolicy policy, NamespaceStore namespaceStore) {
    if (policy == null) {
      throw new IllegalArgumentException("Cannot create ValueRetentionPolicyImpl for null Policy");
    }
    if (policy instanceof InvalidatedRetentionPolicy) {
      return new InvalidatedRetentionPolicyImpl((InvalidatedRetentionPolicy) policy);
    }
    if (policy instanceof LRURetentionPolicy) {
      return new LRURetentionPolicyImpl((LRURetentionPolicy) policy, namespaceStore);
    }
    if (policy instanceof LRWRetentionPolicy) {
      return new LRWRetentionPolicyImpl((LRWRetentionPolicy) policy);
    }
    if (policy instanceof NanosVersionRetentionPolicy) {
      return new NanosVersionRetentionPolicyImpl((NanosVersionRetentionPolicy) policy);
    }
    if (policy instanceof PermanentRetentionPolicy) {
      return new PermanentRetentionPolicyImpl();
    }
    if (policy instanceof TimeAndVersionRetentionPolicy) {
      return new TimeAndVersionRetentionPolicyImpl((TimeAndVersionRetentionPolicy) policy);
    }
    if (policy instanceof ValidOrTimeAndVersionRetentionPolicy) {
      return new ValidOrTimeAndVersionRetentionPolicyImpl((ValidOrTimeAndVersionRetentionPolicy) policy);
    }
    if (policy instanceof NeverRetentionPolicy) {
      return new NeverRetentionPolicyImpl();
    }
    throw new IllegalArgumentException("Missing ValueRetentionPolicy mapping for type " + policy.getClass().getName());
  }
}
