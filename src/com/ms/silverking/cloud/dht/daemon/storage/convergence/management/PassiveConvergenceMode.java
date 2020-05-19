package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

/**
 * Controls how convergence treats passive nodes
 */
public enum PassiveConvergenceMode {

  FullSync_FailOnFailure, FullSync_IgnoreFailures;

  public boolean ignoresFailures() {
    switch (this) {
    case FullSync_FailOnFailure:
      return false;
    case FullSync_IgnoreFailures:
      return true;
    default:
      throw new RuntimeException("Panic");
    }
  }

}