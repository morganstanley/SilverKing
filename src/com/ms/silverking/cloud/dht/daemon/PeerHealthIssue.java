package com.ms.silverking.cloud.dht.daemon;

public enum PeerHealthIssue {
  CommunicationError, OpTimeout, ReplicaTimeout, MissingInZooKeeper, MissingInZooKeeperAfterTimeout, StorageError;

  public boolean isStrongIssue() {
    switch (this) {
    case CommunicationError:
    case StorageError:
      return true;
    default: return false;
    }
  }
}
