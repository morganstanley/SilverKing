package com.ms.silverking.cloud.dht.daemon;

public enum PeerHealthIssue {
  CommunicationError, OpTimeout, ReplicaTimeout, MissingInZooKeeper, MissingInZooKeeperAfterTimeout, StorageError
}
