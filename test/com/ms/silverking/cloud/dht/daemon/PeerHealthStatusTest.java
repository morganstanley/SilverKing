package com.ms.silverking.cloud.dht.daemon;

import static org.junit.Assert.assertTrue;

import java.util.Set;

import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.collection.Pair;
import org.junit.Test;

public class PeerHealthStatusTest {
  @Test
  public void test() {
    PeerHealthStatus s;
    Pair<Set<PeerHealthIssue>, Set<PeerHealthIssue>> ci;

    s = new PeerHealthStatus();
    assertTrue(!s.isStrongSuspect());
    ci = s.getCurrentIssues(SystemTimeUtil.skSystemTimeSource.absTimeMillis());
    assertTrue(ci.getV1().isEmpty());
    assertTrue(ci.getV2().isEmpty());

    s.addIssue(PeerHealthIssue.ReplicaTimeout, SystemTimeUtil.skSystemTimeSource.absTimeMillis());
    assertTrue(!s.isStrongSuspect());
    ci = s.getCurrentIssues(SystemTimeUtil.skSystemTimeSource.absTimeMillis());
    assertTrue(ci.getV1().isEmpty());
    assertTrue(ci.getV2().size() == 1);

    s.addIssue(PeerHealthIssue.StorageError, SystemTimeUtil.skSystemTimeSource.absTimeMillis());
    assertTrue(s.isStrongSuspect());
    ci = s.getCurrentIssues(SystemTimeUtil.skSystemTimeSource.absTimeMillis());
    assertTrue(ci.getV1().size() == 1);
    assertTrue(ci.getV2().size() == 1);

    s.setHealthy(SystemTimeUtil.skSystemTimeSource.absTimeMillis());
    assertTrue(!s.isStrongSuspect());
    ci = s.getCurrentIssues(SystemTimeUtil.skSystemTimeSource.absTimeMillis());
    assertTrue(ci.getV1().size() == 0);
    assertTrue(ci.getV2().size() == 0);
  }
}
