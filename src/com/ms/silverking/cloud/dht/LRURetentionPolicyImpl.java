package com.ms.silverking.cloud.dht;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.NamespaceUtil;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceStore;
import com.ms.silverking.log.Log;
import org.apache.commons.io.FileUtils;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;

public class LRURetentionPolicyImpl extends KeyLevelValueRetentionPolicyImpl<ValueRetentionState.Empty> {
  private static final String stateFileName;
  private static final String stateFileCurrentPointerName;
  private static Weigher<DHTKey, LruRetentionInfo> cacheWeigher;
  private static int stateFileVersion;

  static {
    stateFileName = "lru_retention_state";
    stateFileCurrentPointerName = "current_state_file";
    stateFileVersion = 1;

    cacheWeigher = new Weigher<DHTKey, LruRetentionInfo>() {
      @Override
      public @NonNegative int weigh(@NonNull DHTKey key, @NonNull LruRetentionInfo value) {
        return value.getCompressedSizeBytes();
      }
    };
  }

  // for each key, we need to cache the version and size of the compressed data that key stores
  // To extend this to support multiple versions in future, we will need to cache a fixed-size PQ of version/size pairs
  private final Cache<DHTKey, LruRetentionInfo> retentionMap;
  private final ScheduledExecutorService scheduler;
  private final AtomicInteger nextFileId;

  public LRURetentionPolicyImpl(LRURetentionPolicy policy, NamespaceStore namespaceStore) {
    Path stateDir;
    String nsContextName;

    stateDir = namespaceStore.getNamespaceSSDir().toPath().resolve("lru_persistence");
    nsContextName = NamespaceUtil.contextToDirName(namespaceStore.getNamespaceHash());
    nextFileId = new AtomicInteger();

    if (Files.exists(stateDir)) {
      Log.warningf("Re-loading retention map from existing state in directory %s", stateDir);
      retentionMap = loadFrom(stateDir, policy.getCapacityBytes());
    } else {
      retentionMap = buildEmptyCacheWithCapacity(policy.getCapacityBytes());
    }

    if (policy.getPersistenceIntervalSecs() != LRURetentionPolicy.DO_NOT_PERSIST) {
      if (!stateDir.toFile().exists()) {
        Log.warningf("Persistent LRU state directory does not exist, will create from scratch: %s", stateDir);
        if (!stateDir.toFile().mkdir()) {
          throw new IllegalStateException("Could not create directory " + stateDir.toString());
        }
      }
      scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).setNameFormat(
          nsContextName + "-LRU-%d").build());
      scheduler.scheduleAtFixedRate(() -> persistCurrentLruState(stateDir), policy.getPersistenceIntervalSecs(),
          policy.getPersistenceIntervalSecs(), TimeUnit.SECONDS);
    } else {
      scheduler = null;
    }
  }

  private static Cache<DHTKey, LruRetentionInfo> buildEmptyCacheWithCapacity(long capacityBytes) {
    // Neither guava nor caffeine cache provide an "exact" LRU implementation, the so called "Window TinyLFU policy" is
    // used in the current version of caffeine, see comments @BoundedLocalCache.java#L123
    // see https://github.com/ben-manes/caffeine/wiki/Efficiency for more details
    // There is also a library called caffeine simulator (various policies included) in the github repo, which is not
    // distributed as part of the cache library itself, see https://stackoverflow.com/a/61161161
    return Caffeine.newBuilder().weigher(cacheWeigher).maximumWeight(capacityBytes).recordStats().build();
  }

  private static Cache<DHTKey, LruRetentionInfo> loadFrom(Path stateDir, long capacityBytes) {
    Cache<DHTKey, LruRetentionInfo> initialState = buildEmptyCacheWithCapacity(capacityBytes);
    loadFromStateFile(initialState, stateDir);
    return initialState;
  }

  private static void loadFromStateFile(Cache<DHTKey, LruRetentionInfo> initialState, Path stateDir) {
    File pointerFile;

    pointerFile = stateDir.resolve(stateFileCurrentPointerName).toFile();
    if (pointerFile.exists()) {
      String stateFileName;

      try {
        Path stateFile;

        stateFileName = FileUtils.readFileToString(pointerFile, StandardCharsets.UTF_8);
        stateFile = stateDir.resolve(stateFileName);

        try (FileChannel channel = (FileChannel) Files.newByteChannel(stateFile, StandardOpenOption.READ)) {
          MappedByteBuffer mappedBuffer;
          int keyValueSizeBytes;

          // skip the version number as for now there is only one serialization format
          mappedBuffer = channel.map(FileChannel.MapMode.READ_ONLY, Integer.BYTES, channel.size() - Integer.BYTES);

          // each key is two longs
          // each value is a long and an int
          keyValueSizeBytes = Long.BYTES * 3 + Integer.BYTES;
          // only keep reading while we have at least keyValueSizeBytes remaining in the buffer
          while (mappedBuffer.position() <= mappedBuffer.capacity() - keyValueSizeBytes) {
            DHTKey k;
            LruRetentionInfo v;

            k = new SimpleKey(mappedBuffer.getLong(), mappedBuffer.getLong());
            v = new LruRetentionInfo(mappedBuffer.getLong(), mappedBuffer.getInt());

            initialState.put(k, v);
          }
        } catch (IOException ex) {
          Log.logErrorWarning(ex,
              "Could not read state from state file, will continue with empty state " + stateFileName);
        }
      } catch (IOException ex) {
        Log.logErrorWarning(ex, "Could not read state file pointer, will continue with empty state " + pointerFile);
      }
    }
  }

  private void persistCurrentLruState(Path stateDir) {
    if (retentionMap.estimatedSize() > 0) {
      Path stateFile;
      boolean stateFlushSucceeded;

      do {
        stateFile = stateDir.resolve(stateFileName + "_" + nextFileId.getAndIncrement());
      } while (stateFile.toFile().exists());

      Log.warningf("Going to persist current LRU state to %s with %d entries...", stateFile,
          retentionMap.estimatedSize());
      stateFlushSucceeded = false;
      try (SeekableByteChannel channel = Files.newByteChannel(stateFile, StandardOpenOption.CREATE_NEW,
          StandardOpenOption.WRITE)) {
        int written;
        ByteBuffer keyBuff;
        ByteBuffer valueBuff;
        ByteBuffer versionBuff;

        // write a version marker in case we need to change serialized format later
        versionBuff = ByteBuffer.allocateDirect(Integer.BYTES);
        versionBuff.putInt(stateFileVersion);
        versionBuff.flip();
        channel.write(versionBuff);

        written = 0;
        keyBuff = ByteBuffer.allocateDirect(Long.BYTES * 2);
        valueBuff = ByteBuffer.allocateDirect(Long.BYTES + Integer.BYTES);
        for (Map.Entry<DHTKey, LruRetentionInfo> entry : retentionMap.asMap().entrySet()) {
          DHTKey k;
          LruRetentionInfo v;

          keyBuff.clear();
          valueBuff.clear();
          k = entry.getKey();
          v = entry.getValue();
          keyBuff.putLong(k.getMSL());
          keyBuff.putLong(k.getLSL());
          valueBuff.putLong(v.getVersion());
          valueBuff.putInt(v.getCompressedSizeBytes());
          keyBuff.flip();
          valueBuff.flip();
          channel.write(keyBuff);
          channel.write(valueBuff);

          written++;
        }

        Log.warningf("Wrote %s entries to file %s", written, stateFile);
        stateFlushSucceeded = true;
      } catch (IOException ex) {
        Log.logErrorWarning(ex,
            "Failed to persist LRU info to file " + stateFile + " - will try again after wait period");
      }

      if (stateFlushSucceeded) {
        updateCurrentPointer(stateDir, stateFile.toFile());
      }
    }

    cleanUpDanglingStateFiles(stateDir);
  }

  private static void updateCurrentPointer(Path stateDir, File newStateFile) {
    File nextPointerFile;
    boolean pointerFlushSucceeded;

    nextPointerFile = stateDir.resolve(stateFileCurrentPointerName + ".next").toFile();
    pointerFlushSucceeded = false;

    try {
      FileUtils.writeStringToFile(nextPointerFile, newStateFile.getName(), StandardCharsets.UTF_8);
      pointerFlushSucceeded = true;
    } catch (IOException ex) {
      Log.logErrorWarning(ex, "Failed to persist pointer to new state file into temp file " + nextPointerFile);
    }

    if (pointerFlushSucceeded) {
      Path pointerPath;

      pointerPath = stateDir.resolve(stateFileCurrentPointerName);
      try {
        try {
          Files.move(nextPointerFile.toPath(), pointerPath, StandardCopyOption.REPLACE_EXISTING,
              StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException ex) {
          Files.move(nextPointerFile.toPath(), pointerPath, StandardCopyOption.REPLACE_EXISTING);
        }
      } catch (IOException ex) {
        Log.logErrorWarning(ex, "Failed to move pointer " + nextPointerFile + " to " + pointerPath);
      }
    }
  }

  private static void cleanUpDanglingStateFiles(Path stateDir) {
    File pointerFile;

    pointerFile = stateDir.resolve(stateFileCurrentPointerName).toFile();
    if (pointerFile.exists()) {
      String currentStateFileName;

      try {
        File[] stateDirFiles;

        currentStateFileName = FileUtils.readFileToString(pointerFile, StandardCharsets.UTF_8);
        stateDirFiles = stateDir.toFile().listFiles();

        if (stateDirFiles != null) {
          int removed;

          removed = 0;
          for (File f : stateDirFiles) {
            if (f.getName().startsWith(stateFileName) && !f.getName().equals(currentStateFileName)) {
              Files.delete(f.toPath());
              removed++;
            }
          }

          Log.finef("Cleaned up %d orphaned state files", removed);
        }
      } catch (IOException ex) {
        Log.logErrorWarning(ex, "Error while cleaning up orphaned state files, will retry after wait period.");
      }
    }
  }

  @Override
  public boolean considersStoredLength() {
    return true;
  }

  @Override
  public boolean considersInvalidations() {
    return true;
  }

  @Override
  public ImplementationType getImplementationType() {
    return ImplementationType.SingleReverseSegmentWalk;
  }

  @Override
  public boolean retains(DHTKey key, long version, long creationTimeNanos, boolean invalidated,
      ValueRetentionState.Empty lruRetentionState, long curTimeNanos, int storedLength) {
    LruRetentionInfo retentionInfo;

    // we getIfPresentQuietly in order to avoid affecting the retention of a key when SK calls in to check if it should
    // be reaped or not
    retentionInfo = retentionMap.policy().getIfPresentQuietly(key);

    // we allow SK to retain the key only if the version of that key is at least as recent as that in the retention map
    return retentionInfo != null && version >= retentionInfo.getVersion();
  }

  @Override
  public ValueRetentionState.Empty createInitialState() {
    return ValueRetentionState.EMPTY;
  }

  public void markRead(DHTKey key) {
    retentionMap.getIfPresent(key);
  }

  public void markWrite(DHTKey key, long version, int dataSizeBytes) {
    retentionMap.put(key, new LruRetentionInfo(version, dataSizeBytes));
  }

  // Use for test only for inspecting the content
  public Map<DHTKey, LruRetentionInfo> getCurrentRetentionMap() {
    retentionMap.cleanUp();
    return Collections.unmodifiableMap(retentionMap.asMap());
  }
}

