package com.ms.silverking.cloud.dht.daemon.storage.management;

import java.io.IOException;
import java.util.List;

import com.ms.silverking.cloud.dht.common.DHTKey;

public interface ManagedNamespaceStore {
  PurgeResult syncPurgeKey(DHTKey keyToPurge, long purgeBeforeCreationTimeNanosInclusive) throws IOException;

  PurgeResult syncPurgeKey(DHTKey keyToPurge) throws IOException;

  // Read-only operations to verify key purge
  List<Integer> listKeySegments(DHTKey key) throws IOException;

  List<Integer> listKeySegments(DHTKey key, long beforeCreationTimeNanosInclusive) throws IOException;

  void rollOverHeadSegment(boolean force);
}
