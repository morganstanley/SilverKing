package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.time.AbsMillisTimeSource;
import com.ms.silverking.util.PropertiesHelper;

public abstract class BaseRetrievalEntryState {
  protected static AbsMillisTimeSource absMillisTimeSource;

  private long nextTimeoutAbsMillis;
  private static final int relTimeoutMillis = PropertiesHelper.systemHelper.getInt(
      DHTConstants.internalRelTimeoutMillisProp, 100);
  static final int minRelTimeoutMillis = relTimeoutMillis;

  public static void setAbsMillisTimeSource(AbsMillisTimeSource _absMillisTimeSource) {
    absMillisTimeSource = _absMillisTimeSource;
  }

  public BaseRetrievalEntryState() {
    computeNextReplicaTimeout();
  }

  public abstract IPAndPort getInitialReplica();

  public abstract IPAndPort currentReplica();

  public abstract IPAndPort nextReplica();

  public abstract boolean isComplete();

  public abstract boolean prevReplicaSameAsCurrent();

  protected void computeNextReplicaTimeout() {
    nextTimeoutAbsMillis = relTimeoutMillis >= 0 ?
        absMillisTimeSource.absTimeMillis() + relTimeoutMillis :
        Long.MAX_VALUE;
  }

  public boolean hasTimedOut(long curTimeMillis) {
    return !isComplete() && curTimeMillis > nextTimeoutAbsMillis;
  }
}
