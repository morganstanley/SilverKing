package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.cloud.dht.common.SystemTimeUtil;

public class ReapOnIdleState implements ReapPolicyState {
  private long lastFullReapMillis;
  private long putsAsOfLastFullReap;

  public ReapOnIdleState() {
  }

  public long getLastFullReapMillis() {
    return lastFullReapMillis;
  }

  public long getPutsAsOfLastFullReap() {
    return putsAsOfLastFullReap;
  }

  @Override
  public void fullReapComplete(NamespaceStore nsStore) {
    lastFullReapMillis = SystemTimeUtil.timerDrivenTimeSource.absTimeMillis();
    putsAsOfLastFullReap = nsStore.getNamespaceMetrics().getTotalPuts();
  }
}
