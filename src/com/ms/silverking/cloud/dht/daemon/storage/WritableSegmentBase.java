package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.StorageType;
import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.collection.DHTKeyCuckooBase;
import com.ms.silverking.cloud.dht.collection.DHTKeyIntEntry;
import com.ms.silverking.cloud.dht.collection.IntArrayDHTKeyCuckoo;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.MetaDataUtil;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.SimpleValueCreator;
import com.ms.silverking.cloud.dht.daemon.NodeRingMaster2;
import com.ms.silverking.cloud.dht.daemon.storage.retention.KeyLevelValueRetentionPolicyImpl;
import com.ms.silverking.cloud.dht.daemon.storage.retention.ValueRetentionState;
import com.ms.silverking.collection.Quintuple;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.collection.cuckoo.CuckooBase;
import com.ms.silverking.collection.cuckoo.IntArrayCuckoo;
import com.ms.silverking.collection.cuckoo.TableFullException;
import com.ms.silverking.collection.cuckoo.WritableCuckooConfig;
import com.ms.silverking.log.Log;
import com.ms.silverking.text.StringUtil;
import com.ms.silverking.util.ArrayUtil;

abstract class WritableSegmentBase extends AbstractSegment implements ReadableWritableSegment {
  private final AtomicInteger nextFree;
  protected final int dataSegmentSize;
  protected final int indexOffset;

  protected DHTKeyCuckooBase keyToOffset;
  protected CuckooBase<Integer> offsetToLength;

  protected final int segmentNumber; // zero-based
  protected final File nsDir;

  protected static final int dataOffset = SegmentFormat.headerSize;

  private static final boolean debug = false;
  private static final boolean debugCompaction = false;
  private static final boolean debugRetention = false;

  // called from openReadOnly
  WritableSegmentBase(File nsDir, int segmentNumber, ByteBuffer dataBuf, DHTKeyCuckooBase keyToOffset,
      Set<Integer> invalidatedOffsets, CuckooBase<Integer> offsetToLength, OffsetListStore offsetListStore, int dataSegmentSize) throws IOException {
    super(dataBuf, offsetListStore, invalidatedOffsets != null ? ImmutableSet.copyOf(invalidatedOffsets) : null);
    this.segmentNumber = segmentNumber;
    this.keyToOffset = keyToOffset;
    nextFree = new AtomicInteger(SegmentFormat.headerSize);
    this.nsDir = nsDir;
    this.dataSegmentSize = dataSegmentSize;
    this.indexOffset = dataSegmentSize;
    this.offsetToLength = offsetToLength;
    if (debug) {
      Log.warning("WritableSegmentBase created for read only: ", nsDir);
    }
  }

  // called from Create
  WritableSegmentBase(File nsDir, int segmentNumber, ByteBuffer dataBuf, WritableCuckooConfig initialCuckooConfig,
      int dataSegmentSize, NamespaceOptions nsOptions, CuckooBase<Integer> offsetToLength) {
    super(dataBuf, new RAMOffsetListStore(nsOptions), new HashSet<>());
    this.segmentNumber = segmentNumber;
    this.keyToOffset = new IntArrayDHTKeyCuckoo(initialCuckooConfig);
    nextFree = new AtomicInteger(SegmentFormat.headerSize);
    this.nsDir = nsDir;
    this.dataSegmentSize = dataSegmentSize;
    this.indexOffset = dataSegmentSize;
    this.offsetToLength = offsetToLength;
    if (debug) {
      Log.warning("WritableSegmentBase created for writing/reading: ", nsDir);
    }
  }

  // called from RAMSegment.create
  WritableSegmentBase(File nsDir, int segmentNumber, ByteBuffer dataBuf, WritableCuckooConfig initialCuckooConfig,
      int dataSegmentSize, NamespaceOptions nsOptions) {
    this(nsDir, segmentNumber, dataBuf, initialCuckooConfig, dataSegmentSize, nsOptions, null);
  }

  public int getDataSegmentSize() {
    return dataSegmentSize;
  }

  int getSegmentNumber() {
    return segmentNumber;
  }

  public DHTKeyCuckooBase getPKC() {
    return keyToOffset;
  }

  @Override
  protected int getRawOffset(DHTKey key) {
    return keyToOffset.get(key);
  }

  void setNextFree(int nextFree) {
    try {
      dataBuf.position(nextFree);
    } catch (IllegalArgumentException iae) {
      Log.warning(dataBuf.toString() + "\t" + nextFree);
      throw iae;
    }
    this.nextFree.set(nextFree);
  }

  public int freeStorageSpaceRaw() {
    return dataSegmentSize - nextFree.get();
  }

  int getStoredLength(int offset) {
    if (offsetToLength == null) {
      return super.getStoredLength(offset);
    } else {
      return offsetToLength.get(offset);
    }
  }

  public SegmentStorageResult putFormattedValue(DHTKey key, ByteBuffer formattedBuf, StorageParameters storageParams,
      NamespaceOptions nsOptions) {
    int writeOffset;

    if (debugPut) {
      Log.warning("putFormattedBuf: ", KeyUtil.keyToString(key) + "\t" + storageParams);
    }
    writeOffset = StorageFormat.writeFormattedValueToBuf(key, formattedBuf, dataBuf, nextFree, dataSegmentSize);
    if (writeOffset != StorageFormat.writeFailedOffset) {
      SegmentStorageResult segmentStorageResult;

      segmentStorageResult = _put(key, writeOffset, storageParams.getVersion(), storageParams.getValueCreator(), nsOptions);
      processStorageResult(key, segmentStorageResult, writeOffset, storageParams);
      return segmentStorageResult;
    } else {
      return SegmentStorageResult.segmentFull;
    }
  }

  public SegmentStorageResult put(DHTKey key, ByteBuffer value, StorageParameters storageParams, byte[] userData,
      NamespaceOptions nsOptions) {
    int writeOffset;

    if (debugPut) {
      Log.warning("put: ", KeyUtil.keyToString(key) + "\t" + storageParams);
    }
    writeOffset = StorageFormat.writeToBuf(key, value, storageParams, userData, dataBuf, nextFree, dataSegmentSize,
        true);
    if (writeOffset != StorageFormat.writeFailedOffset) {
      SegmentStorageResult segmentStorageResult;

      segmentStorageResult = _put(key, writeOffset, storageParams.getVersion(), storageParams.getValueCreator(),
          nsOptions);
      processStorageResult(key, segmentStorageResult, writeOffset, storageParams);
      return segmentStorageResult;
    } else {
      return SegmentStorageResult.segmentFull;
    }
  }

  private void processStorageResult(DHTKey key, SegmentStorageResult segmentStorageResult, int writeOffset, StorageParameters storageParams) {
      if (segmentStorageResult == SegmentStorageResult.stored) {
      if (storageParams.isInvalidation()) {
        if (invalidatedOffsets != null) {
          invalidatedOffsets.add(writeOffset);
        }
      }
      if (offsetToLength != null) {
        int storedLength;

        storedLength = MetaDataUtil.getStoredLength((ByteBuffer)dataBuf.duplicate(), writeOffset + DHTKey.BYTES_PER_KEY);
        if (Log.levelMet(Level.FINE)) {
          Log.finef("%s => %d %d", KeyUtil.keyToString(key), writeOffset, storedLength);
        }
        try {
          offsetToLength.put(writeOffset, storedLength);
        } catch (TableFullException tfe) {
          IntArrayCuckoo _offsetToLength;

          Log.finef("offsetToLength full; rehashing");
          _offsetToLength = (IntArrayCuckoo) offsetToLength;
          _offsetToLength.rehashAndAdd(_offsetToLength, writeOffset, storedLength);
          offsetToLength = _offsetToLength;
    }
  }
    }
  }

  public SegmentStorageResult _put(DHTKey key, int offset, long version, byte[] valueCreator,
      NamespaceOptions nsOptions) {
    OffsetList offsetList;
    int existingOffset;

    existingOffset = keyToOffset.get(key);
    if (debugPut) {
      Log.warning("segmentNumber: ", segmentNumber);
      Log.warning("existingOffset: ", existingOffset);
    }
    if (existingOffset == DHTKeyCuckooBase.keyNotFound) {
      // no offset for the key; add the mapping
      if (nsOptions.getVersionMode() == NamespaceVersionMode.SINGLE_VERSION || nsOptions.getStorageType() == StorageType.RAM) {
        if (debugPut) {
          Log.warning("initial mapping: ", KeyUtil.keyToString(key));
        }
        try {
          keyToOffset.put(key, offset);
          if (debugPut) {
            if (keyToOffset.get(key) != offset) {
              Log.warning("sanity check failed" + keyToOffset.get(key) + " " + offset);
            }
          }
        } catch (TableFullException tfe) {
          Log.warning("Segment pkc full. Creating new table");
          keyToOffset = IntArrayDHTKeyCuckoo.rehashAndAdd((IntArrayDHTKeyCuckoo) keyToOffset, key, offset);
        }
      } else {
        long creationTime;

        // Recovery takes too long if we need to look all over for the version
        // For disk-based ns, we currently always create an offset list
        // Similar logic in NamespaceStore.putSegmentNumberAndVersion()
        offsetList = offsetListStore.newOffsetList();
        if (nsOptions.getRevisionMode() == RevisionMode.UNRESTRICTED_REVISIONS) {
          creationTime = getCreationTime(offset);
        } else {
          creationTime = 0;
        }

        offsetList.putOffset(version, offset, creationTime);
        try {
          keyToOffset.put(key, -((RAMOffsetList) offsetList).getIndex());
        } catch (TableFullException tfe) {
          Log.warning("Segment pkc full. Creating new table");
          keyToOffset = IntArrayDHTKeyCuckoo.rehashAndAdd((IntArrayDHTKeyCuckoo) keyToOffset, key,
              -((RAMOffsetList) offsetList).getIndex());
        }
      }
    } else {
      // this key exists in pkc, we next determine whether it has
      // a single value associated with it or an offset list
      if (nsOptions.getVersionMode() == NamespaceVersionMode.SINGLE_VERSION) {
        //long    existingVersion;

        //existingVersion = getVersion(existingOffset);
        //if (version != existingVersion) {
        //    return SegmentStorageResult.invalidVersion;
        //} else {
        byte[] existingChecksum;
        byte[] newChecksum;

        existingChecksum = getChecksum(existingOffset);
        newChecksum = getChecksum(offset);
        if (ArrayUtil.compareSigned(existingChecksum, newChecksum, ArrayUtil.MismatchedLengthMode.Ignore) == 0) {
          return SegmentStorageResult.stored;
        } else {
          if (debugPut) {
            Log.warning(
                String.format("Checksums failed to compare: %s %s", StringUtil.byteArrayToHexString(existingChecksum),
                    StringUtil.byteArrayToHexString(newChecksum)));
          }
          return SegmentStorageResult.mutation;
        }
        //}
      } else {
        if (existingOffset >= 0) {
          long existingVersion;
          long existingCreationTime;
          long creationTime;

          if (debugPut) {
            Log.warning("single key associated: ", KeyUtil.keyToString(key));
          }
          // single key is associated, create an offset list
          offsetList = offsetListStore.newOffsetList();
          existingVersion = getVersion(existingOffset);

          if (nsOptions.getRevisionMode() == RevisionMode.UNRESTRICTED_REVISIONS) {
            existingCreationTime = getCreationTime(existingOffset);
            creationTime = getCreationTime(offset);
          } else {
            existingCreationTime = 0;
            creationTime = 0;
          }

          if (nsOptions.getRevisionMode() == RevisionMode.UNRESTRICTED_REVISIONS || version > existingVersion) {
            offsetList.putOffset(existingVersion, existingOffset, existingCreationTime);
            offsetList.putOffset(version, offset, creationTime);
            if (debugPut) {
              Log.warning("removing existing mapping: ", KeyUtil.keyToString(key));
            }
            boolean removed;
            removed = keyToOffset.remove(key);
            if (debugPut || Log.levelMet(Level.FINE)) {
              Log.warning("removed: ", removed);
              Log.warning("pkc.get: ", keyToOffset.get(key));
              Log.warning("putting new mapping: ",
                  KeyUtil.keyToString(key) + " " + -((RAMOffsetList) offsetList).getIndex());
            }
            try {
              keyToOffset.put(key, -((RAMOffsetList) offsetList).getIndex());
            } catch (TableFullException tfe) {
              Log.warning("Segment pkc full. Creating new table");
              keyToOffset = IntArrayDHTKeyCuckoo.rehashAndAdd((IntArrayDHTKeyCuckoo) keyToOffset, key,
                  -((RAMOffsetList) offsetList).getIndex());
            }
          } else {
            ValueCreator creator;

            // FUTURE - Think about this. Important currently to allow for retries to succeed cleanly.
            creator = getCreator(offset);
            if (SimpleValueCreator.areEqual(creator.getBytes(), valueCreator)) {
              byte[] existingChecksum;
              byte[] newChecksum;

              existingChecksum = getChecksum(existingOffset);
              newChecksum = getChecksum(offset);
              if (ArrayUtil.compareSigned(existingChecksum, newChecksum) == 0) {
                //Log.warningf("pkc.getTotalEntries() %d", pkc.getTotalEntries());
                //Log.warningf("%s %d %d", key, existingOffset, offset);
                //Log.warningf("%s %d %d", key, existingVersion, version);
                return SegmentStorageResult.duplicateStore;
              } else {
                if (debugPut) {
                  Log.warning(String.format(
                      "Duplicate existingVersion %d version %d, eo %d o %d, but checksums failed to compare: %s %s",
                      existingVersion, version, existingOffset, offset,
                      StringUtil.byteArrayToHexString(existingChecksum), StringUtil.byteArrayToHexString(newChecksum)));
                }
                return SegmentStorageResult.invalidVersion;
              }
            } else {
              // FUTURE: Consider: allow puts of incomplete stores to continue?
              if (debugPut) {
                Log.warning("WritableSegmentBase._put detected invalid version b");
                Log.warning(nsOptions);
              }
              return SegmentStorageResult.invalidVersion;
            }
          }
        } else {
          if (debugPut) {
            Log.warning("list associated: ", KeyUtil.keyToString(key));
          }
          // offset list is associated, use the existing offset list
          offsetList = offsetListStore.getOffsetList(-existingOffset);
          if (nsOptions.getRevisionMode() == RevisionMode.UNRESTRICTED_REVISIONS || version >= offsetList.getLatestVersion()) {
            // note: > since we want new versions to be added
            // == since we might need to store over an incomplete store
            // so >= to cover both
            long creationTime;

            if (debugPut || Log.levelMet(Level.FINE)) {
              Log.warning("adding offset: ", KeyUtil.keyToString(key) + " " + offset);
            }

            if (nsOptions.getRevisionMode() == RevisionMode.UNRESTRICTED_REVISIONS) {
              creationTime = getCreationTime(offset);
            } else {
              creationTime = 0;
            }
            offsetList.putOffset(version, offset, creationTime);
          } else {
            if (debugPut) {
              Log.warningf("version %d offsetList.getLatestVersion() %d", version, offsetList.getLatestVersion());
            }
            return SegmentStorageResult.invalidVersion;
          }
        }
      }
    }

    // sanity check for debugging only
    //if (pkc.get(key) != offset) {
    //    throw new RuntimeException("failed sanity check");
    //}
    return SegmentStorageResult.stored;
  }

  public abstract void persist() throws IOException;

  public OpResult putUpdate(DHTKey key, long version, byte storageState, boolean ignoreLock) {
    int offset;

    if (debugPut) {
      Log.warning("putUpdate: ", KeyUtil.keyToString(key) + "\t" + version + "\t" + storageState);
    }
    offset = getResolvedOffset(key, VersionConstraint.exactMatch(version));
    if (offset == noSuchKey) {
      Log.warning("putUpdate couldn't getResolvedOffset: ", KeyUtil.keyToString(key));
      return OpResult.ERROR;
    } else {
      offset += DHTKey.BYTES_PER_KEY;
      if (!ignoreLock && MetaDataUtil.getLockSeconds(dataBuf, offset) > 0) {
        return OpResult.LOCKED; // overloading the intent here to relay the lock request
      } else {
        MetaDataUtil.updateStorageState(dataBuf, offset, storageState);
        return OpResult.SUCCEEDED;
      }
    }
  }

  /////////////////////////

  private static class OffsetVersionAndCreationTimeListReverseComparator
      implements Comparator<Triple<Integer, Long, Long>> {
    @Override
    public int compare(Triple<Integer, Long, Long> t1, Triple<Integer, Long, Long> t2) {
      if (t1.getV1() < t2.getV1()) {
        return 1; // reverse order
      } else if (t1.getV1() > t2.getV1()) {
        return -1; // reverse order
      } else {
        if (t1.getV2() < t2.getV2()) {
          return 1; // reverse order
        } else if (t1.getV2() > t2.getV2()) {
          return -1; // reverse order
        } else {
          if (t1.getV3() < t2.getV3()) {
            return 1; // reverse order
          } else if (t1.getV3() > t2.getV3()) {
            return -1; // reverse order
          } else {
            return 0;
          }
        }
      }
    }
  }

  public <T extends ValueRetentionState>
    Quintuple<CompactionCheckResult, Set<Integer>, Set<Integer>, Set<DHTKey>, Boolean> singleReverseSegmentWalk(
      KeyLevelValueRetentionPolicyImpl<T> vrp, T valueRetentionState, long curTimeNanos, NodeRingMaster2 ringMaster,
      Set<DHTKey> invalidatedAndCompactedKeys,
      boolean returnStoredLength) {
    int numRetained;
    int numDiscarded;
    Set<Integer> retainedOffsets;
    Set<Integer> discardedOffsets;
    Set<DHTKey> invalidatedKeys;
    boolean     matchedInvalidatedKeys;
    int  retainedBytes;

    numRetained = 0;
    numDiscarded = 0;
    retainedBytes = 0;
    retainedOffsets = new HashSet<>();
    discardedOffsets = new HashSet<>();
    invalidatedKeys = new HashSet<>();
    matchedInvalidatedKeys = false;
    // Within each segment, the backwards walking is per key.
    // The outer loop loops through the keys first.
    for (DHTKeyIntEntry entry : keyToOffset) {
      int rawOffset;
      List<Triple<Integer, Long, Long>> offsetVersionAndCreationTimeList;

      rawOffset = entry.getValue();
      if (rawOffset >= 0) {
        long version;
        long creationTime;

        version = getVersion(rawOffset);
        creationTime = getCreationTime(rawOffset);
        offsetVersionAndCreationTimeList = ImmutableList.of(new Triple<>(rawOffset, version, creationTime));
      } else {
        offsetVersionAndCreationTimeList = new ArrayList(
            ImmutableList.copyOf(offsetListStore.getOffsetList(-rawOffset).offsetVersionAndStorageTimeIterable()));
        Collections.sort(offsetVersionAndCreationTimeList, new OffsetVersionAndCreationTimeListReverseComparator());
      }
      // List is now in reverse order; iterate down through the offsets
      for (Triple<Integer, Long, Long> offsetVersionAndCreationTime : offsetVersionAndCreationTimeList) {
        DHTKey entryKey;
        int offset;
        long version;
        long creationTime;
        int consideredLength;
        int storedLength;

        offset = offsetVersionAndCreationTime.getV1();
        version = offsetVersionAndCreationTime.getV2();
        creationTime = offsetVersionAndCreationTime.getV3();
        entryKey = entry.getKey();

        if (returnStoredLength || vrp.considersStoredLength()) {
        storedLength = getStoredLength(offset);
        } else {
          storedLength = 0;
        }

        if (vrp.considersStoredLength()) {
          consideredLength = storedLength;
        } else {
          consideredLength = 0;
        }
        if (Log.levelMet(Level.FINE)) {
          Log.finef("consideredLength %s %d", KeyUtil.keyToString(entry.getKey()), consideredLength);
        }
        if (debugRetention) {
          Log.warningf("%s %d %d %d %s", entry.getKey(), offset, 0/*getCreationTime(offset)*/, curTimeNanos,
              isInvalidation(offset));
        }
        // FUTURE - the isInvalidated() call below may touch disk and dramatically increase the
        // execution time. Not a huge deal for segments that will be modified, but
        // a dramatic increase in time for segments that won't.
        // Leaving for now as tracking in memory requires space
        if (vrp.retains(entryKey, version, creationTime, isInvalidation(offset), valueRetentionState, curTimeNanos,
            consideredLength) && (ringMaster == null || ringMaster.iAmPotentialReplicaFor(entryKey, false))) {
          ++numRetained;
          retainedOffsets.add(offset);
          retainedBytes += storedLength;
          if (!matchedInvalidatedKeys) {
            if (invalidatedAndCompactedKeys.contains(entryKey)) {
              // If we retain a key that a higher-numbered segment has marked for invalidation and compaction
              // then we must record that fact
              matchedInvalidatedKeys = true;
            }
          }
          if (debugRetention) {
            Log.warningf("Retained %s\t%d", entry.getKey(), offset);
          }
        } else {
          ++numDiscarded;
          discardedOffsets.add(offset);
          invalidatedKeys.add(entry.getKey());
          if (debugRetention) {
            Log.warningf("Discarded %s\t%d", entry.getKey(), offset);
          }
        }
      }
    }
    return Quintuple.of(new CompactionCheckResult(numRetained, numDiscarded, retainedBytes), retainedOffsets, discardedOffsets,
        invalidatedKeys, matchedInvalidatedKeys);
  }
}
