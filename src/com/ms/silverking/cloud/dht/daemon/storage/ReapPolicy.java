package com.ms.silverking.cloud.dht.daemon.storage;

public interface ReapPolicy<T extends ReapPolicyState> {
  @Deprecated
  enum EmptyTrashMode {Never, BeforeInitialReap, BeforeAndAfterInitialReap, EveryPartialReap, EveryFullReap};

  T createInitialState();

  boolean reapAllowed(T state, NamespaceStore nsStore, ReapPhase reapPhase, boolean isStartup);

  @Deprecated
  EmptyTrashMode getEmptyTrashMode();

  int getIdleReapPauseMillis();

  boolean supportsLiveReap();

  int getReapIntervalMillis();

  int getBatchLimit(ReapPhase reapPhase);

  boolean verboseReap();

  boolean verboseReapPhase();

  boolean verboseSegmentDeletionAndCompaction();
}
