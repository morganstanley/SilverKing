package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.SegmentIndexLocation;
import com.ms.silverking.cloud.dht.daemon.storage.FileSegment.SegmentPrereadMode;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.StorageProtocolUtil;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.collection.HashedSetMap;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Quadruple;
import com.ms.silverking.log.Log;

public class NamespaceFileSegmentCompactor implements FileSegmentCompactor {
  private final File  nsDir;
  private final NamespaceOptions nsOptions;
  private Pair<FileSegment, Integer> currentCompactionDest;
  private Set<Integer> currentCompactionSourceSegments;
  private int prevSegmentCompacted;
  private Cache<Integer, FileSegment> fileSegmentCache;
  private boolean isFirstSegment;

  private static final int  noPrevSegmentCompacted = Integer.MIN_VALUE;

  private static final boolean verbose = false;

  public NamespaceFileSegmentCompactor(File nsDir, NamespaceOptions nsOptions) {
    this.nsDir = nsDir;
    this.nsOptions = nsOptions;
    currentCompactionSourceSegments = new HashSet<>();
    prevSegmentCompacted = noPrevSegmentCompacted;
    isFirstSegment = true;
  }

  @Override
  public void setFileSegmentCache(Cache<Integer, FileSegment> fileSegmentCache) {
    this.fileSegmentCache = fileSegmentCache;
  }

  /*
   * Current strategy for compaction within a segment, is to leave the segment size constant,
   * and to count on sparse file support for actual disk savings. That is, upon compaction, the
   * data will be compacted at the start of the data segment, and the index will remain at the
   * end of the data segment. In between, sparse file support should allow the file system to
   * omit allocation of bytes.
   *
   * Across segments, any contiquous run of compactable segments will be merged to the extent possible.
   * Merging always preserves the storage order of values.
   */

  /**
   * Create an empty FileSegment for compaction
    */
  FileSegment createCompactedSegment(int segmentNumber) {
    try {
      Log.warningf("createCompactedSegment %d", segmentNumber);
      return FileSegment.create(FileCompactionUtil.getCompactionDir(nsDir), segmentNumber, nsOptions.getSegmentSize(),
          FileSegment.SyncMode.NoSync, nsOptions);
    } catch (IOException ioe) {
      Log.logErrorWarning(ioe, "createCompactedSegment failed: " + segmentNumber);
      throw new RuntimeException(ioe);
    }
  }

  private ExtractedLiveValues extractLiveValues(int sourceSegmentNumber, EntryRetentionCheck retentionCheck,
      boolean includeStorageTime) throws IOException {
    DataSegmentWalker dsWalker;
    FileSegment sourceSegment;
    List<Quadruple<DHTKey, ByteBuffer, StorageParameters, NamespaceOptions>>  liveValues;
    int storedLength;
    HashedSetMap<DHTKey, CompactorModifiedEntry> removedEntries;

    Log.warningf("extractLiveValues %d", sourceSegmentNumber);
    storedLength = 0;
    removedEntries = new HashedSetMap<>();
    liveValues = new ArrayList<>();
    sourceSegment = FileSegment.openReadOnly(nsDir, sourceSegmentNumber, nsOptions.getSegmentSize(), nsOptions,
        SegmentIndexLocation.RAM, SegmentPrereadMode.Preread);

    dsWalker = new DataSegmentWalker(sourceSegment.dataBuf);
    for (DataSegmentWalkEntry entry : dsWalker) {
      if (verbose) {
        Log.warning(entry);
      }
      if (!StorageProtocolUtil.storageStateValidForRead(nsOptions.getConsistencyProtocol(), entry.getStorageState())) {
        if (verbose) {
          Log.warningf("Ignoring invalid storage state %s %d", entry.getKey(), entry.getStorageState());
        }
      } else {
        if (retentionCheck.shouldRetain(sourceSegmentNumber, entry)) {
          if (verbose) {
            Log.warning("setting: " + entry.getOffset());
            Log.warning("sanity check: " + sourceSegment.getPKC().get(entry.getKey()));
            Log.warning("Retaining:\t", entry.getKey());
          }
          liveValues.add(Quadruple.of(entry.getKey(), entry.getStoredFormat(),
              entry.getStorageParameters(), nsOptions));
          storedLength += StorageFormat.storageLengthOfFormattedValue(entry.getStoredFormat());
        } else {
          if (verbose) {
            Log.warning("Dropping: \t", entry.getKey());
          }
          removedEntries.addValue(entry.getKey(),
              CompactorModifiedEntry.newRemovedEntry(entry.getVersion(), sourceSegmentNumber,
                  includeStorageTime ? entry.getCreationTime() : 0));
        }
      }
    }
    return new ExtractedLiveValues(storedLength, liveValues, removedEntries);
  }

  private HashedSetMap<DHTKey, CompactorModifiedEntry> addToCompactedSegment(FileSegment destSegment,
      List<Quadruple<DHTKey, ByteBuffer, StorageParameters, NamespaceOptions>> liveValues, int sourceSegmentNumber,
      boolean logCompaction) {
    HashedSetMap<DHTKey, CompactorModifiedEntry>  movedEntries;

    if (logCompaction) {
      Log.warningf("addToCompactedSegment %d", destSegment.getSegmentNumber());
    }
    movedEntries = new HashedSetMap<>();
    for (Quadruple<DHTKey, ByteBuffer, StorageParameters, NamespaceOptions> liveValue : liveValues) {
      SegmentStorageResult storageResult;

      storageResult = destSegment.putFormattedValue(liveValue.getV1(), liveValue.getV2(), liveValue.getV3(), liveValue.getV4());
      if (storageResult == SegmentStorageResult.stored) {
        StorageParameters sp;

        sp = liveValue.getV3();
        if (verbose || Log.levelMet(Level.FINE)) {
          Log.warningf("moving value: %s %d => %d  %s", KeyUtil.keyToString(liveValue.getV1()), sourceSegmentNumber,
              destSegment.getSegmentNumber(),
              CompactorModifiedEntry.newModifiedEntry(sp.getVersion(), sourceSegmentNumber, 0,
                  destSegment.getSegmentNumber()));
        }
        movedEntries.addValue(liveValue.getV1(), CompactorModifiedEntry.newModifiedEntry(sp.getVersion(), sourceSegmentNumber,
            nsOptions.getRevisionMode() == RevisionMode.UNRESTRICTED_REVISIONS ? sp.getCreationTime() : 0,
            destSegment.getSegmentNumber()));
      } else {
        // FUTURE - think about duplicate stores, and the duplicate store WritableSegmentBase
        if (storageResult != SegmentStorageResult.duplicateStore) {
          throw new RuntimeException("Compaction failed: " + storageResult);
        } else {
          Log.warningf("Duplicate store in compaction %s", liveValue.getV1());
        }
      }
    }
    return movedEntries;
  }

  private boolean segmentUncompactedInBetweenExclusive(int startSegmentNumber, int endSegmentNumber,
      Set<Integer> uncompactedSegments) {
    if (endSegmentNumber > startSegmentNumber + 1) {
      for (int i = startSegmentNumber + 1; i < endSegmentNumber; i++) {
        if (uncompactedSegments.contains(i)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public HashedSetMap<DHTKey, CompactorModifiedEntry> compact(int segmentNumber, EntryRetentionCheck retentionCheck,
      boolean logCompaction, boolean hasInvalidEntries, Set<Integer> uncompactedSegments) throws IOException {
    HashedSetMap<DHTKey, CompactorModifiedEntry> modifiedEntries;
    ExtractedLiveValues extractedLiveValues;
    boolean createNewCompactionDest;

    if (logCompaction) {
      Log.warningf("Compacting segment: %d %s", segmentNumber, nsDir.getName());
    }
    modifiedEntries = new HashedSetMap<>();

    if (!isFirstSegment && !hasInvalidEntries && currentCompactionDest == null) {
      Log.warningf("Ignoring segment that can't compact into previous (no target): %d %s", segmentNumber, nsDir.getName());
      return null; // This segment was added only because it may compact into a previous, but no previous target set
    }
    isFirstSegment = false;

    // get the live values from the source segment
    extractedLiveValues = extractLiveValues(segmentNumber, retentionCheck, false);

    // Check to see if we need to create a new target segment, or if we can use the current
    if (currentCompactionDest == null) {
      if (logCompaction) {
        Log.warning("No currentCompactionDest");
      }
      createNewCompactionDest = true;
    } else if (extractedLiveValues.getStoredLength() > currentCompactionDest.getV1().freeStorageSpaceRaw()) {
      if (logCompaction) {
        Log.warningf("%d > %d", extractedLiveValues.getStoredLength(), currentCompactionDest.getV1().freeStorageSpaceRaw());
        Log.warningf("Can't fit extractedLiveValues into segment %d", currentCompactionDest.getV2());
      }
      createNewCompactionDest = true;
    } else if (prevSegmentCompacted != noPrevSegmentCompacted
        && segmentUncompactedInBetweenExclusive(prevSegmentCompacted, segmentNumber, uncompactedSegments)) {
      if (logCompaction) {
        Log.warningf("Skipped segment(s) detected %d %d. Using new compaction dest", prevSegmentCompacted, segmentNumber);
      }
      createNewCompactionDest = true;
    } else {
      if (logCompaction) {
        Log.warningf("Using currentCompactionDest %d", currentCompactionDest.getV2());
      }
      createNewCompactionDest = false;
    }
    prevSegmentCompacted = segmentNumber;

    if (createNewCompactionDest) {
      // If there is a current target segment, persist it
      persistCurrentIfExists(logCompaction);
      if (logCompaction) {
        Log.warningf("Creating new currentCompactionDest as segment %d", segmentNumber);
      }
      currentCompactionDest = new Pair<>(createCompactedSegment(segmentNumber), segmentNumber);
    }
    try {
      HashedSetMap<DHTKey, CompactorModifiedEntry>  movedEntries;

      movedEntries = addToCompactedSegment(currentCompactionDest.getV1(), extractedLiveValues.getValues(),
          segmentNumber, logCompaction);
      if (Log.levelMet(Level.FINE)) {
        Log.finef("removedEntries %s", CollectionUtil.toString(extractedLiveValues.getRemovedEntries().keySet()));
      }
      modifiedEntries.addAll(extractedLiveValues.getRemovedEntries());
      if (Log.levelMet(Level.FINE)) {
        Log.finef("movedEntries %s", movedEntries.keySet());
      }
      modifiedEntries.addAll(movedEntries);
    } catch (Exception e) {
      Log.logErrorWarning(e, "Unexpected failure adding extracted values");
      Log.warningf("Unable to compact %d", segmentNumber);
      // We attempt to carry on operation.
      // Halt compaction at the current target, and skip compaction of this segment.
      persistCurrentIfExists(logCompaction);
      // We are unsure if anything was compacted here, so return an empty map.
      return new HashedSetMap<>();
    }

    // All live data in the source segment is now in the target segment; ensure that the current destination is synced
    // to disk, then add source segment to the set of segments to be deleted
    // (We always start with source == target; thus the old segment is deleted first. Should we fail part way,
    // the recovery logic can recover. See recoverOngoingCompaction() for details.)
    currentCompactionDest.getV1().sync();
    if (segmentNumber != currentCompactionDest.getV2()) {
      // We omit the target segment to ensure that it is not deleted prematurely
      // All others we add so that they will be deleted later on
      currentCompactionSourceSegments.add(segmentNumber);
    }

    if (logCompaction) {
      Log.warning("Done compacting segment: ", segmentNumber);
    }
    return modifiedEntries;
  }

  /**
   * Flush the compacted segment to disk and replace the uncompacted segment with the compacted segment
   */
  private void persistCurrentIfExists(boolean logCompaction) throws IOException {
    if (currentCompactionDest != null) {
      File oldFile;
      File newFile;
      int segmentNumber;

      // Note: we do not need to hold a lock here:
      // - We do not update the valueSegments map in NamespaceStore; all key => segment mappings
      //   remain the same for now. Those mappings are valid because:
      //   - Mappings to source segments are still valid as the source segments still exist;
      //     we delete them elsewhere (with a lock)
      //   - Mappings to the compaction segment are still valid as the new compacted segment contains
      //     all valid values that the original did before
      // - Note that even after the file replacement below, the cache may retain a reference to
      //   the original uncompacted file. That's ok for now; it has all valid values as mentioned above.
      //   (We must flush the uncompacted segment from cache when we delete the source segments
      //   as the uncompacted does not have the newly added values.)
      segmentNumber = currentCompactionDest.getV2();
      logCompaction = true; // FUTURE - reap/compaction logging needs to be overhauled
      if (logCompaction) {
        Log.warningf("Persisting segment: %d %s", segmentNumber, nsDir.getName());
      }
      currentCompactionDest.getV1().persist();
      currentCompactionDest = null;
      if (logCompaction) {
        Log.warning("Swapping to compacted segment: ", segmentNumber);
      }
      oldFile = FileSegment.fileForSegment(nsDir, segmentNumber);
      newFile = FileCompactionUtil.getCompactionFile(nsDir, segmentNumber);
      FileCompactionUtil.rename(newFile, oldFile);
      fileSegmentCache.invalidate(segmentNumber);
      if (logCompaction) {
        Log.warning("Done swapping to compacted segment: ", segmentNumber);
      }
    }
  }

  @Override
  public Set<Integer> drainCurrentCompactionSourceSegments() {
    Set<Integer>  ccss;

    ccss = ImmutableSet.copyOf(currentCompactionSourceSegments);
    currentCompactionSourceSegments = new HashSet<>();
    return ccss;
  }

  // lock must be held
  @Override
  public void flushCompaction(boolean logCompaction) {
    try {
      isFirstSegment = true;
      persistCurrentIfExists(logCompaction);
    } catch (IOException ioe) {
      Log.logErrorWarning(ioe);
      throw new RuntimeException(ioe);
    }
  }

  private static class ExtractedLiveValues {
    private final int storedLength;
    private final List<Quadruple<DHTKey, ByteBuffer, StorageParameters, NamespaceOptions>>  values;
    private final HashedSetMap<DHTKey, CompactorModifiedEntry>  removedEntries;

    ExtractedLiveValues(int storedLength,
        List<Quadruple<DHTKey, ByteBuffer, StorageParameters, NamespaceOptions>> values,
        HashedSetMap<DHTKey, CompactorModifiedEntry> removedEntries) {
      this.storedLength = storedLength;
      this.values = values;
      this.removedEntries = removedEntries;
    }

    public int getStoredLength() {
      return storedLength;
    }

    public List<Quadruple<DHTKey, ByteBuffer, StorageParameters, NamespaceOptions>> getValues() {
      return values;
    }

    public HashedSetMap<DHTKey, CompactorModifiedEntry> getRemovedEntries() {
      return removedEntries;
    }
  }

  static void recoverOngoingCompaction(File nsDir) throws IOException {
    List<Integer> compactionSegments;

    Log.warningf("recoverOngoingCompaction %s", nsDir);
    compactionSegments = FileCompactionUtil.getCompactSegments(nsDir);
    if (compactionSegments.size() > 1) {
      throw new RuntimeException("Unexpected multiple segments found in compaction dir for "+ nsDir);
    } else {
      if (compactionSegments.size() == 1) {
        File compactedFile;
        File uncompactedFile;
        int compactedSegmentNumber;

        compactedSegmentNumber = compactionSegments.get(0);
        Log.warningf("Found ongoing compaction %s %d", nsDir, compactedSegmentNumber);
        compactedFile = FileCompactionUtil.getCompactionFile(nsDir, compactedSegmentNumber);
        uncompactedFile = new File(nsDir, Integer.toString(compactedSegmentNumber));
        if (uncompactedFile.exists()) {
          // Note that this is safe as we always delete files in monotonically increasing order
          // That is, the existence of the uncompacted segment n implies that all segments > n
          // have not been deleted (as part of this compaction; they may have been deleted previously).
          // Thus, deleting the compacted file will not lose any data.
          Log.warningf("Uncompacted file %s exists. Deleting compaction file %s",
              uncompactedFile, compactedFile);
          compactedFile.delete();
        } else {
          Log.warningf("Uncompacted file does not exist. Moving compaction file %s => %s",
              compactedFile, uncompactedFile);
          compactedFile.renameTo(uncompactedFile);
        }
      } else {
        Log.warningf("No ongoing compaction found %s", nsDir);
      }
    }
    Log.warningf("recoverOngoingCompaction complete %s", nsDir);
  }
}
