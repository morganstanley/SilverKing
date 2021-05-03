package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.IOException;
import java.util.Set;

import com.google.common.cache.Cache;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.collection.HashedSetMap;

/**
 * This interface only exists as a testing artifact. There is no interface/implementation split
 * required by the implementation proper.
 * FUTURE - consider removal
 */
public interface FileSegmentCompactor {
  HashedSetMap<DHTKey, CompactorModifiedEntry> compact(int segmentNumber, EntryRetentionCheck retentionCheck,
      boolean logCompaction, boolean hasInvalidEntries, Set<Integer> uncompactedSegments) throws IOException;
  public void setFileSegmentCache(Cache<Integer, FileSegment> fileSegmentCache);

  Set<Integer> drainCurrentCompactionSourceSegments();

  void flushCompaction(boolean logCompaction);
}
