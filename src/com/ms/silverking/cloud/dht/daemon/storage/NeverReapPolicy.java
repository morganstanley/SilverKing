package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.text.ObjectDefParser2;

public class NeverReapPolicy implements ReapPolicy<ReapPolicyState> {
  public static NeverReapPolicy instance = new NeverReapPolicy();

  static {
    ObjectDefParser2.addParser(instance);
  }

  public NeverReapPolicy() {
  }

  @Override
  public ReapPolicyState createInitialState() {
    return null;
  }

  @Override
  public boolean reapAllowed(ReapPolicyState state, NamespaceStore nsStore, ReapPhase reapPhase, boolean isStartup) {
    return false;
  }

  @Override
  public com.ms.silverking.cloud.dht.daemon.storage.ReapPolicy.EmptyTrashMode getEmptyTrashMode() {
    return EmptyTrashMode.Never;
  }

  @Override
  public int getIdleReapPauseMillis() {
    return 0;
  }

  @Override
  public boolean supportsLiveReap() {
    return false;
  }

  @Override
  public int getReapIntervalMillis() {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getBatchLimit(ReapPhase reapPhase) {
    return 0;
  }

  @Override
  public boolean verboseReap() {
    return false;
  }

  @Override
  public boolean verboseReapPhase() {
    return false;
  }

  @Override
  public boolean verboseSegmentDeletionAndCompaction() {
    return false;
  }
}
