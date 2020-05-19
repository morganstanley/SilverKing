package com.ms.silverking.cloud.skfs.dir.serverside;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.storage.StorageValueAndParameters;
import com.ms.silverking.cloud.dht.serverside.PutTrigger;
import com.ms.silverking.cloud.dht.serverside.RetrieveTrigger;
import com.ms.silverking.cloud.dht.serverside.SSNamespaceStore;
import com.ms.silverking.cloud.dht.serverside.SSRetrievalOptions;
import com.ms.silverking.cloud.dht.serverside.SSStorageParameters;
import com.ms.silverking.cloud.dht.serverside.SSStorageParametersAndRequirements;
import com.ms.silverking.cloud.skfs.dir.DirectoryBase;
import com.ms.silverking.cloud.skfs.dir.DirectoryInMemory;
import com.ms.silverking.cloud.skfs.dir.DirectoryInPlace;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.compression.CompressionUtil;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.process.SafeThread;
import com.ms.silverking.text.StringUtil;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;

public class DirectoryServer implements PutTrigger, RetrieveTrigger {
  private SSNamespaceStore nsStore;
  private final ConcurrentMap<DHTKey, BaseDirectoryInMemorySS> directories;
  private Set<DHTKey> directoriesOnDiskAtBoot;
  private File logDir;
  private NamespaceOptions nsOptions;

  private enum Mode {Eager, Lazy}

  ;

  public static String modeProperty = DirectoryServer.class.getCanonicalName() + ".Mode";
  private static final Mode defaultMode = Mode.Lazy;
  private static final Mode mode;

  private static final boolean debug = false;
  private static final boolean debugMergePut = false;

  static FileDeletionWorker fileDeletionWorker = new FileDeletionWorker();

  static {
    String modeValue;
    Mode _mode;

    Log.warning("Initialized DirectoryServer class");
    modeValue = PropertiesHelper.systemHelper.getString(modeProperty, UndefinedAction.ZeroOnUndefined);
    if (modeValue == null) {
      _mode = defaultMode;
    } else {
      try {
        _mode = Mode.valueOf(modeValue);
      } catch (Exception e) {
        Log.logErrorWarning(e, "Using default mode " + defaultMode);
        _mode = defaultMode;
      }
    }
    mode = _mode;
    Log.warningf("DirectoryServer mode %s", mode);
  }

  /*
   * Note: DirectoryServer presently accepts all first stage put operations as valid, and eagerly merges in
   * the changes observed. As a result, putUpdate() is a nop.
   */

  public DirectoryServer() {
    directories = new ConcurrentHashMap<>();
  }

  @Override
  public void initialize(SSNamespaceStore nsStore) {
    this.nsStore = nsStore;
    this.logDir = nsStore.getNamespaceSSDir();
    this.nsOptions = nsStore.getNamespaceOptions();
    directoriesOnDiskAtBoot = getDirectoriesOnDiskAtBoot();
    new Persister();
  }

  private Set<DHTKey> getDirectoriesOnDiskAtBoot() {
    Set<DHTKey> dirs;

    dirs = new HashSet<>();
    for (String s : logDir.list()) {
      try {
        if (!filterEmptyDir(s)) {
          dirs.add(KeyUtil.keyStringToKey(s));
        }
      } catch (Exception e) {
        Log.logErrorWarning(e);
        Log.warningf("DirectoryServer.getDirectoriesOnDiskAtBoot() skipping %s", s);
      }
    }
    return ImmutableSet.copyOf(dirs);
  }

  private boolean filterEmptyDir(String s) {
    File f;

    f = new File(logDir, s);
    if (f.list().length == 0) {
      Log.warningf("Removing empty dir: %s", s);
      f.delete();
      return true;
    } else {
      return false;
    }
  }

  private BaseDirectoryInMemorySS newDirectoryInMemorySS(DHTKey dirKey, DirectoryBase d,
      SSStorageParameters storageParams, File sDir, NamespaceOptions nsOptions) {
    return newDirectoryInMemorySS(dirKey, d, storageParams, sDir, nsOptions, true);
  }

  private BaseDirectoryInMemorySS newDirectoryInMemorySS(DHTKey dirKey, DirectoryBase d,
      SSStorageParameters storageParams, File sDir, NamespaceOptions nsOptions, boolean reap) {
    if (mode == Mode.Lazy) {
      return new LazyDirectoryInMemorySS(dirKey, d, storageParams, sDir, nsOptions, reap);
    } else {
      return new EagerDirectoryInMemorySS(dirKey, d, storageParams, sDir, nsOptions, reap);
    }
  }

  @Override
  public OpResult put(SSNamespaceStore nsStore, DHTKey key, ByteBuffer value,
      SSStorageParametersAndRequirements storageParams, byte[] userData, NamespaceVersionMode nsVersionMode) {
    //Stopwatch    sw;

    //sw = new SimpleStopwatch();
    //try {
    byte[] buf;
    int bufOffset;
    int bufLimit;
    BaseDirectoryInMemorySS existingDir;
    DirectoryInPlace updateDir;

    if (debug || Log.levelMet(Level.INFO)) {
      Log.warningf("DirectoryServer.put() %s %s %s", KeyUtil.keyToString(key), value.hasArray(),
          storageParams.getCompression());
    }

    value = BufferUtil.ensureArrayBacked(value);
    buf = value.array();
    bufOffset = value.arrayOffset() + value.position();
    bufLimit = value.limit();
    //System.out.printf("%s\n", value);
    //System.out.printf("%d %d %d\n", buf.length, bufOffset, bufLimit);
    if (storageParams.getCompression() == Compression.NONE) {
      //System.out.printf("No compression\n");
    } else {
      try {
        int dataOffset;

        //System.out.printf("Compression\n");
        dataOffset = value.position();
        buf = CompressionUtil.decompress(storageParams.getCompression(), value.array(), dataOffset,
            storageParams.getCompressedSize(), storageParams.getUncompressedSize());
        bufOffset = 0;
        bufLimit = buf.length;
      } catch (IOException ioe) {
        Log.logErrorWarning(ioe);
        return OpResult.ERROR;
      }
    }
    updateDir = new DirectoryInPlace(buf, bufOffset, bufLimit);
    // put() holds a write lock so no concurrency needs to be handled.
    // We still, however, look for existing directories on disk
    return _put(key, storageParams, updateDir);
    //} finally {
    //    sw.stop();
    //    Log.warningf("DirectoryServer.put()\t%f", sw.getElapsedSeconds());
    //}
  }

  // must hold write lock
  private OpResult _put(DHTKey key, SSStorageParameters storageParams, DirectoryBase updateDir) {
    BaseDirectoryInMemorySS existingDir;

    existingDir = getExistingDirectory(key, true);
    if (existingDir == null) {
      BaseDirectoryInMemorySS newDir;

      newDir = newDirectoryInMemorySS(key, updateDir, storageParams, new File(logDir, KeyUtil.keyToString(key)),
          nsStore.getNamespaceOptions());
      if (newDir instanceof LazyDirectoryInMemorySS) {
        ((LazyDirectoryInMemorySS) newDir).setUnserialized();
      }
      directories.put(key, newDir);
    } else {
      existingDir.update(updateDir, storageParams);
    }
    return OpResult.SUCCEEDED;
  }

  @Override
  public OpResult putUpdate(SSNamespaceStore nsStore, DHTKey key, long version, byte storageState) {
    return OpResult.SUCCEEDED;
  }

  @Override
  public Map<DHTKey, OpResult> mergePut(List<StorageValueAndParameters> values) {
    Map<DHTKey, Pair<DirectoryInMemory, MergedStorageParameters>> mergedUpdates;
    Map<DHTKey, OpResult> results;
    int numMerged;
    Stopwatch sw;

    if (debugMergePut || Log.levelMet(Level.INFO)) {
      sw = new SimpleStopwatch();
    } else {
      sw = null;
    }
    numMerged = 0;
    Log.info("mergePut");
    mergedUpdates = new HashMap<>();
    for (StorageValueAndParameters svp : values) {
      try {
        Pair<DirectoryInMemory, MergedStorageParameters> mergedDirAndSP;
        DirectoryInMemory update;

        update = svpToDirectoryInMemory(svp);
        mergedDirAndSP = mergedUpdates.get(svp.getKey());
        if (mergedDirAndSP == null) {
          MergedStorageParameters mergedSP;

          if (Log.levelMet(Level.INFO)) {
            Log.warningf("new mergeSP %s %s", KeyUtil.keyToString(svp.getKey()),
                IPAddrUtil.addrToString(svp.getValueCreator(), 0));
          }
          mergedSP = new MergedStorageParameters(svp);
          mergedUpdates.put(svp.getKey(), new Pair<>(update, mergedSP));
        } else {
          DirectoryInMemory mergedDir;
          MergedStorageParameters mergedSP;

          ++numMerged;
          if (Log.levelMet(Level.INFO)) {
            Log.warningf("merging into mergeSP %s %s", KeyUtil.keyToString(svp.getKey()),
                IPAddrUtil.addrToString(svp.getValueCreator(), 0));
          }
          mergedDir = mergedDirAndSP.getV1();
          mergedDir.update(update);
          mergedSP = mergedDirAndSP.getV2().merge(svp);
          mergedUpdates.put(svp.getKey(), new Pair<>(mergedDir, mergedSP));
        }
      } catch (IOException ioe) {
        Log.logErrorWarning(ioe, "Ignoring update due to compression error " + svp);
      } catch (RuntimeException re) {
        Log.logErrorWarning(re, "Ignoring update due to error " + svp);
      }
    }
    if (debugMergePut || Log.levelMet(Level.INFO)) {
      sw.stop();
      Log.warningf("merged %d of %d in %f", numMerged, values.size(), sw.getElapsedSeconds());
      sw.reset();
    }

    nsStore.getReadWriteLock().writeLock().lock();
    try {
      results = put(mergedUpdates);
    } finally {
      nsStore.getReadWriteLock().writeLock().unlock();
    }
    if (debugMergePut || Log.levelMet(Level.INFO)) {
      sw.stop();
      Log.warningf("put for merge complete in %f", sw.getElapsedSeconds());
      sw.reset();
    }
    return results;
  }

  @Override
  public boolean supportsMerge() {
    return true;
  }

  private Map<DHTKey, OpResult> put(Map<DHTKey, Pair<DirectoryInMemory, MergedStorageParameters>> updates) {
    Lock writeLock;
    Map<DHTKey, OpResult> results;

    results = new HashMap<>();
    writeLock = nsStore.getReadWriteLock().writeLock();
    writeLock.lock();
    try {
      for (Map.Entry<DHTKey, Pair<DirectoryInMemory, MergedStorageParameters>> entry : updates.entrySet()) {
        OpResult result;

        result = _put(entry.getKey(), entry.getValue().getV2(), entry.getValue().getV1());
        results.put(entry.getKey(), result);
      }
    } finally {
      writeLock.unlock();
    }
    return results;
  }

  private DirectoryInMemory svpToDirectoryInMemory(StorageValueAndParameters svp) throws IOException {
    ByteBuffer value;
    byte[] buf;
    SSStorageParameters storageParams;
    int dataOffset;

    storageParams = svp;
    value = svp.getValue();
    value = BufferUtil.ensureArrayBacked(value);
    dataOffset = value.position();
    if (storageParams.getCompression() == Compression.NONE) {
      buf = new byte[svp.getUncompressedSize()];
      System.arraycopy(value.array(), dataOffset, buf, 0, buf.length);
    } else {
      buf = CompressionUtil.decompress(storageParams.getCompression(), value.array(), dataOffset,
          storageParams.getCompressedSize(), storageParams.getUncompressedSize());
    }
    return new DirectoryInMemory(new DirectoryInPlace(buf));
  }

  @Override
  public ByteBuffer retrieve(SSNamespaceStore nsStore, DHTKey key, SSRetrievalOptions options) {
    BaseDirectoryInMemorySS existingDir;
    ByteBuffer rVal;

    if (debug || Log.levelMet(Level.INFO)) {
      Log.warningf("retrieve %s", KeyUtil.keyToString(key));
    }
    existingDir = getExistingDirectory(key, false);
    if (existingDir != null) {
      //Log.warning("existingDir found");
      rVal = existingDir.retrieve(options);
    } else {
      //Log.warning("existingDir not found");
      rVal = null;
    }
    if (debug) {
      Log.warningf("rVal %s %s", rVal, StringUtil.byteBufferToHexString(rVal));
    }
    return rVal;
  }

  @Override
  public ByteBuffer[] retrieve(SSNamespaceStore nsStore, DHTKey[] keys, SSRetrievalOptions options) {
    // Re-use non-batched retrieve for now
    ByteBuffer[] buffers = new ByteBuffer[keys.length];
    for (int i = 0; i < buffers.length; i++) {
      DHTKey key = keys[i];
      buffers[i] = retrieve(nsStore, key, options);
    }
    return buffers;
  }

  /**
   * Check to see if given directory is already in memory. If not,
   * check to see if it exists on disk; if so, then read it in to memory.
   *
   * @param key
   * @return the given DirectoryInMemorySS if it exists in memory or was found on disk
   */
  public BaseDirectoryInMemorySS getExistingDirectory(DHTKey key, boolean reapOnRecover) {
    BaseDirectoryInMemorySS existingDir;

    existingDir = directories.get(key);
    if (existingDir == null) {
      File dir;

      dir = new File(logDir, KeyUtil.keyToString(key));
      if (dir.exists()) {
        BaseDirectoryInMemorySS prev;

        Log.warningAsyncf("DirectoryServer.getExistingDirectory() recovering %s", KeyUtil.keyToString(key));
        existingDir = newDirectoryInMemorySS(key, null, null, dir, nsOptions, reapOnRecover);
        // This may be called by retrieve() which only holds a read lock.
        // As a result, we need to worry about concurrency.
        prev = directories.putIfAbsent(key, existingDir);
        if (prev != null) {
          existingDir = prev;
        }
      } else {
        existingDir = null;
      }
    }
    return existingDir;
  }

  private Set<DHTKey> getUnionKeySet() {
    Set<DHTKey> keys;

    keys = new HashSet<>();
    keys.addAll(directoriesOnDiskAtBoot);
    keys.addAll(directories.keySet());
    return ImmutableSet.copyOf(keys);
  }

  @Override
  public Iterator<DHTKey> keyIterator() {
    return getUnionKeySet().iterator();
  }

  @Override
  public long getTotalKeys() {
    return getUnionKeySet().size();
  }

  private class Persister implements Runnable {
    private final Thread pThread;

    private static final long checkIntervalMillis = 1 * 1000;

    Persister() {
      pThread = new SafeThread(this, "DirectoryServer.Persister", true);
      pThread.start();
    }

    public void run() {
      while (true) {
        try {
          checkForPersistence();
          ThreadUtil.sleep(checkIntervalMillis);
        } catch (Exception e) {
          Log.logErrorWarning(e, "Unexpected exception in DirectoryServer.Persister");
          ThreadUtil.sleep(checkIntervalMillis);
        }
      }
    }

    private void checkForPersistence() {
      long checkTimeMillis;
      List<File> filesToRemove;

      Log.info("checkForPersistence()");
      filesToRemove = new ArrayList<>();
      checkTimeMillis = SystemTimeUtil.skSystemTimeSource.absTimeMillis();
      for (BaseDirectoryInMemorySS dir : directories.values()) {
        List<File> dirFilesToRemove;

        dirFilesToRemove = dir.checkForPersistence(checkTimeMillis, nsStore);
        filesToRemove.addAll(dirFilesToRemove);
      }
      deleteFiles(filesToRemove);
      Log.info("out checkForPersistence()");
    }

    private void deleteFiles(List<File> filesToRemove) {
      if (filesToRemove.size() > 0) {
        // Write lock this namespace to ensure that no retrieval operations are ongoing
        // (retrieval operations may lazily realize the directory in ram from the serialized file)
        nsStore.getReadWriteLock().writeLock().lock();
        try {
          fileDeletionWorker.delete(filesToRemove);
        } finally {
          nsStore.getReadWriteLock().writeLock().unlock();
        }
      }
    }
  }

  @Override
  public boolean subsumesStorage() {
    return true;
  }
}
