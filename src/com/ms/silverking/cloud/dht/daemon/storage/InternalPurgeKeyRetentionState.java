package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.cloud.dht.ValueRetentionState;

/**
 * Only used in server side (within package scope)
 */
class InternalPurgeKeyRetentionState implements ValueRetentionState {
  private long count;
  private long latestCreationTimeNanos;
  private long latestVersion;

  InternalPurgeKeyRetentionState() {
    reset();
  }

  void reset() {
    this.count = 0;
    this.latestCreationTimeNanos = -1;
    this.latestVersion = -1;
  }

  void keyPurged(long creationTimeNanos, long version) {
    count++;
    if (creationTimeNanos > latestCreationTimeNanos) {
      latestCreationTimeNanos = creationTimeNanos;
    }
    if (version > latestVersion) {
      latestVersion = version;
    }
  }

  long getCount() {
    return count;
  }

  /**
   * Caller shall be careful about its return value
   *
   * @return latest creationTimeNanos of all purged versions; Or negative long if nothing purged (getCount() == 0)
   */
  long getLatestCreationTimeNanos() {
    return latestCreationTimeNanos;
  }

  /**
   * Caller shall be careful about its return value
   *
   * @return latest version of all purged versions; Or negative long if nothing purged (getCount() == 0)
   */
  long getLatestVersion() {
    return latestVersion;
  }
}
