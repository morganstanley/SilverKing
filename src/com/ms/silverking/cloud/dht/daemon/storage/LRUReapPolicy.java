package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.text.ObjectDefParser2;

/**
 * Reap policy intended for use on SK instances where an LRU value retention policy is in use
 */
public class LRUReapPolicy extends BaseReapPolicy<LRUReapPolicyState> {
  private final int idleReapPauseMillis;
  private final int reapIntervalMillis;
  private final int batchLimit;

  private static final int defaultIdleReapPauseMillis = 5;
  private static final int defaultReapIntervalMillis = 1 * 1000;
  private static final int defaultBatchLimit = 1000;
  private static final boolean defaultLruVerboseReap = false;

  private static final boolean debug = false;

  private static final LRUReapPolicy template = new LRUReapPolicy();

  static {
    ObjectDefParser2.addParser(template);
  }

  public LRUReapPolicy() {
    this(defaultLruVerboseReap, defaultVerboseReapPhase, defaultVerboseSegmentDeletion, defaultIdleReapPauseMillis,
        defaultReapIntervalMillis, defaultBatchLimit);
  }

  public LRUReapPolicy(boolean verboseReap, boolean verboseReapPhase, boolean verboseSegmentDeletion,
      int idleReapPauseMillis, int reapIntervalMillis, int batchLimit) {
    super(verboseReap, verboseReapPhase, verboseSegmentDeletion);
    this.idleReapPauseMillis = idleReapPauseMillis;
    this.reapIntervalMillis = reapIntervalMillis;
    this.batchLimit = batchLimit;
  }

  @Override
  public LRUReapPolicyState createInitialState() {
    return new LRUReapPolicyState();
  }

  @Override
  public boolean reapAllowed(LRUReapPolicyState state, NamespaceStore nsStore, ReapPhase reapPhase, boolean isStartup) {
    if (isStartup) {
      return true;
    } else {
      if (reapPhase != ReapPhase.compactAndDelete) {
        return true;
      } else {
        // Leave open the possibility of adding additional logic here to reduce compact and delete pauses
        return true;
      }
    }
  }

  @Override
  public com.ms.silverking.cloud.dht.daemon.storage.ReapPolicy.EmptyTrashMode getEmptyTrashMode() {
    return EmptyTrashMode.EveryPartialReap;
  }

  @Override
  public int getIdleReapPauseMillis() {
    return idleReapPauseMillis;
  }

  @Override
  public boolean supportsLiveReap() {
    return true;
  }

  @Override
  public int getReapIntervalMillis() {
    return reapIntervalMillis;
  }

  @Override
  public int getBatchLimit(ReapPhase reapPhase) {
    if (reapPhase == null) {
      return batchLimit;
    } else {
      switch (reapPhase) {
      case reap:
        return Integer.MAX_VALUE;
      default:
        return batchLimit;
      }
    }
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }
}
