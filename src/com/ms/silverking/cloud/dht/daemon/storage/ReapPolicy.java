package com.ms.silverking.cloud.dht.daemon.storage;

public interface ReapPolicy<T extends ReapPolicyState> {
  public enum EmptyTrashMode {Never, BeforeInitialReap, BeforeAndAfterInitialReap, EveryPartialReap, EveryFullReap}

  ;

  public T createInitialState();

  public boolean reapAllowed(T state, NamespaceStore nsStore, ReapPhase reapPhase, boolean isStartup);

  public EmptyTrashMode getEmptyTrashMode();

  public int getIdleReapPauseMillis();

  public boolean supportsLiveReap();

  public int getReapIntervalMillis();

  public int getBatchLimit(ReapPhase reapPhase);

  public boolean verboseReap();

  public boolean verboseReapPhase();

  public boolean verboseSegmentDeletionAndCompaction();
}
