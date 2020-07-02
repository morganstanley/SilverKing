package com.ms.silverking.cloud.dht.daemon;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.log.Log;

/**
 * Tracks the health of a peer.
 */
class PeerHealthStatus {
  private final Map<PeerHealthIssue,Long> healthReports;
  private long  healthyTimeMillis;

  private static final boolean  debug = false;

  /*
  As we will have an instance of this class for every replica, we make some effort to keep memory
  consumption low. We use the object monitor in place of a lock to avoid creating a new object and to avoid
  the overhead of concurrent structures.
  */

  private static final long weakErrorTimeoutMillis = 5 * 60 * 1000;

  PeerHealthStatus() {
    healthReports = new HashMap<>(PeerHealthIssue.values().length);
  }

  public void addIssue(PeerHealthIssue issue, long timeMillis) {
    synchronized (this) {
      healthReports.put(issue, timeMillis);
    }
  }

  public void setHealthy(long timeMillis) {
    synchronized (this) {
      this.healthyTimeMillis = timeMillis;
    }
  }

  public boolean isStrongSuspect() {
    return getCurrentIssues(SystemTimeUtil.timerDrivenTimeSource.absTimeMillis()).getV1().size() > 0;
  }

  public Pair<Set<PeerHealthIssue>,Set<PeerHealthIssue>> getCurrentIssues(long curTimeMillis) {
    synchronized (this) {
      return getIssues(curTimeMillis, healthyTimeMillis + 1);
    }
  }

  /**
   * Return issues that have occurred >= sinceTimeMillis. curTimeMillis is used to compute
   * weak error timeouts
   * @param curTimeMillis
   * @param sinceTimeMillis
   * @return
   */
  private Pair<Set<PeerHealthIssue>,Set<PeerHealthIssue>> getIssues(long curTimeMillis, long sinceTimeMillis) {
    Set<PeerHealthIssue> strongIssues;
    Set<PeerHealthIssue> weakIssues;

    strongIssues = new HashSet<>();
    weakIssues = new HashSet<>();
    for (Map.Entry<PeerHealthIssue,Long> report : healthReports.entrySet()) {
      if (report.getValue() >= sinceTimeMillis) {
        if (report.getKey().isStrongIssue()) {
          strongIssues.add(report.getKey());
        } else if (curTimeMillis - report.getValue() <= weakErrorTimeoutMillis) {
          weakIssues.add(report.getKey());
        }
      }
    }
    if (debug || Log.levelMet(Level.FINE)) {
      Log.warningf("strongIssues: %s", strongIssues);
      Log.warningf("weakIssues: %s", weakIssues);
    }
    return Pair.of(ImmutableSet.copyOf(strongIssues),ImmutableSet.copyOf(weakIssues));
  }
}
