package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.collection.HashedSetMap;
import com.ms.silverking.collection.Triple;

public interface FileSegmentCompactor {
  List<Integer> getTrashSegments(File nsDir) throws IOException;

  List<Integer> getCompactSegments(File nsDir) throws IOException;

  HashedSetMap<DHTKey, Triple<Long, Integer, Long>> compact(File nsDir, int segmentNumber, NamespaceOptions nsOptions,
      EntryRetentionCheck retentionCheck, boolean logCompaction) throws IOException;

  void delete(File nsDir, int segmentNumber) throws IOException;

  /**
   * Try to hard-delete "trash" and "compact" files from disk
   * <b>NOTE: <b/> No guarantee to delete each file (Will simply logging message if file is not deleted)
   *
   * @param nsDir namespace base dir
   * @return estimated deleted file number (might be greater than actual deletion number)
   */
  int emptyTrashAndCompaction(File nsDir);

  /**
   * Force to hard-delete "trash" and "compact" files from disk
   * <b>NOTE: <b/> Guarantee to delete each file (will throw IOException if can't do so)
   *
   * @param nsDir namespace base dir
   * @return actual deleted file number
   * @throws IOException if fail to delete any file; Or other issue happens during deletion
   */
  int forceEmptyTrashAndCompaction(File nsDir) throws IOException;
}
