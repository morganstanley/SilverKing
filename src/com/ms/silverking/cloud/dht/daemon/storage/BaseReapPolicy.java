package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;

abstract class BaseReapPolicy<T extends ReapPolicyState> implements ReapPolicy<T> {
  protected final boolean verboseReap;
  protected final boolean verboseReapPhase;
  protected final boolean verboseSegmentDeletionAndCompaction;

  protected static final boolean defaultVerboseReap = true;
  protected static final boolean defaultVerboseReapPhase = false;
  protected static final boolean defaultVerboseSegmentDeletion = true;

  static {
    ObjectDefParser2.addParserWithExclusions(BaseReapPolicy.class, null, FieldsRequirement.ALLOW_INCOMPLETE, null);
  }

  public BaseReapPolicy(boolean verboseReap, boolean verboseReapPhase, boolean verboseSegmentDeletionAndCompaction) {
    this.verboseReap = verboseReap;
    this.verboseReapPhase = verboseReapPhase;
    this.verboseSegmentDeletionAndCompaction = verboseSegmentDeletionAndCompaction;
  }

  public BaseReapPolicy() {
    this(defaultVerboseReap, defaultVerboseReapPhase, defaultVerboseSegmentDeletion);
  }

  @Override
  public boolean verboseReap() {
    return verboseReap;
  }

  @Override
  public boolean verboseReapPhase() {
    return verboseReapPhase;
  }

  @Override
  public boolean verboseSegmentDeletionAndCompaction() {
    return verboseSegmentDeletionAndCompaction;
  }
}
