package com.ms.silverking.cloud.skfs.dir.serverside;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.daemon.storage.StorageParameters;
import com.ms.silverking.cloud.dht.serverside.SSNamespaceStore;
import com.ms.silverking.cloud.dht.serverside.SSRetrievalOptions;
import com.ms.silverking.cloud.dht.serverside.SSStorageParameters;
import com.ms.silverking.cloud.dht.serverside.SSUtil;
import com.ms.silverking.cloud.skfs.dir.DirectoryBase;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.log.Log;

/**
 * Lazy extension of BaseDirectoryInMemorySS. Creates serialized directories on read.
 */
public class LazyDirectoryInMemorySS extends BaseDirectoryInMemorySS {
  private final Lock serializationLock;
  private volatile boolean hasUnserializedUpdates;
  private boolean firstUpdateReceived;

  private static final boolean debug = false;

  LazyDirectoryInMemorySS(DHTKey dirKey, DirectoryBase d, SSStorageParameters storageParams, File sDir,
      NamespaceOptions nsOptions, boolean reap) {
    super(dirKey, d, storageParams, sDir, nsOptions, reap, false);
    serializationLock = new ReentrantLock();
  }

  public LazyDirectoryInMemorySS(DHTKey dirKey, DirectoryBase d, SSStorageParameters storageParams, File sDir,
      NamespaceOptions nsOptions) {
    this(dirKey, d, storageParams, sDir, nsOptions, true);
  }

  public void setUnserialized() {
    // used when creating a new dir with entries
    hasUnserializedUpdates = true;
  }

  /**
   * NamespaceStore writeLock held at this point
   * - no other update() calls in progress
   * - no retrieve() calls in progress
   * - Persister() may be in progress
   */
  public void update(DirectoryBase update, SSStorageParameters sp) {
    boolean mutated;

    mutated = update(update);
    //System.out.printf("update from %s %s\n", new IPAndPort(sp.getValueCreator()).toString(), mutated);
    //update.display();
    if (mutated) {
      latestUpdateSP = sp;
      hasUnserializedUpdates = true;
    } else {
      if (getNumEntries() == 0 && !firstUpdateReceived) {
        latestUpdateSP = sp;
        hasUnserializedUpdates = true;
        firstUpdateReceived = true;
      }
    }
  }

  protected final void persistLatestIfNecessary(SSNamespaceStore nsStore) {
    SerializedDirectory sd;

    // FUTURE - dedup w.r.t. retrieve
    if (hasUnserializedUpdates) {
      nsStore.getReadWriteLock().writeLock().lock();
      serializationLock.lock();
      try {
        // We must double check now to see if updates were serialized while this thread was waiting for the lock
        if (hasUnserializedUpdates) {
          Pair<SSStorageParameters, byte[]> sdsp;
          SerializedDirectory prev;

          // Not yet serialized. Do it now
          sdsp = serializeDir();
          sd = new SerializedDirectory(sdsp, false);
          if (Log.levelMet(Level.INFO)) {
            Log.warningf("a serializedVersions.put %s %d", KeyUtil.keyToString(dirKey), sdsp.getV1().getVersion());
          }
          prev = serializedVersions.putIfAbsent(sdsp.getV1().getVersion(), sd);
          if (prev != null) {
            Log.warningAsyncf("Unexpected multiple serialization for %s %d", KeyUtil.keyToString(dirKey),
                sdsp.getV1().getVersion());
          }
          hasUnserializedUpdates = false;
          persist(sd);
        } else {
          // Updates were serialized while this thread was waiting for the lock. Simply return the most recent
          if (Log.levelMet(Level.INFO)) {
            Log.warningf("b serializedVersions.get %s %d", KeyUtil.keyToString(dirKey), latestUpdateSP.getVersion());
          }
          sd = getMostRecentDirectory();
        }
      } finally {
        serializationLock.unlock();
        nsStore.getReadWriteLock().writeLock().unlock();
      }
    }
  }

  /**
   * NamespaceStore readLock held at this point
   * - no update() calls in progress
   * - other retrieve() calls may be in progress
   * - Persister() may be in progress
   */
  public ByteBuffer retrieve(SSRetrievalOptions options) {
    ByteBuffer rVal;
    VersionConstraint vc;
    SerializedDirectory sd;
    Pair<SSStorageParameters, ByteBuffer> sdp;

    vc = options.getVersionConstraint();
    // Check to see if this retrieve() is asking for the latest version
    if (vc.equals(
        VersionConstraint.greatest) || (latestUpdateSP != null && vc.getMode() == VersionConstraint.Mode.GREATEST && vc.getMax() == latestUpdateSP.getVersion())) {
      // This update is asking for the latest version
      if (hasUnserializedUpdates) {
        // There are updates to the parent DirectoryInMemory that have not yet been serialized
        if (!options.getRetrievalType().hasValue()) {
          StorageParameters sp;

          sp = serializeDirMetaData();
          // Retrieval requires meta data only. For this case, don't bother serializing the underlying value
          // Just create dummy metadata.
          sd = new SerializedDirectory(sp, SSUtil.metaDataToStoredValue(sp), false);
          if (debug || Log.levelMet(Level.INFO)) {
            Log.warningf("_ metaData %s %d", KeyUtil.keyToString(dirKey), sd.getStorageParameters().getVersion());
          }
        } else {
          // Retrieval requires a fully serialized value. Lock to ensure that we only create one.
          serializationLock.lock();
          try {
            // We must double check now to see if updates were serialized while this thread was waiting for the lock
            if (hasUnserializedUpdates) {
              Pair<SSStorageParameters, byte[]> sdsp;
              SerializedDirectory prev;

              // Not yet serialized. Do it now
              sdsp = serializeDir();
              sd = new SerializedDirectory(sdsp, false);
              if (debug || Log.levelMet(Level.INFO)) {
                Log.warningf("a serializedVersions.put %s %d", KeyUtil.keyToString(dirKey), sdsp.getV1().getVersion());
              }
              prev = serializedVersions.putIfAbsent(sdsp.getV1().getVersion(), sd);
              if (prev != null) {
                Log.warningAsyncf("Unexpected multiple serialization for %s %d", KeyUtil.keyToString(dirKey),
                    sdsp.getV1().getVersion());
                if (sd.getStorageParameters().getUncompressedSize() > prev.getStorageParameters().getUncompressedSize()) {
                  Log.warningAsyncf("Replacing with larger %s %d", KeyUtil.keyToString(dirKey),
                      sdsp.getV1().getVersion());
                  serializedVersions.put(sdsp.getV1().getVersion(), sd);
                }
              }
              hasUnserializedUpdates = false;
              persist(sd);
            } else {
              // Updates were serialized while this thread was waiting for the lock. Simply return the most recent
              if (debug || Log.levelMet(Level.INFO)) {
                Log.warningf("b serializedVersions.get %s %d", KeyUtil.keyToString(dirKey),
                    latestUpdateSP.getVersion());
              }
              sd = getMostRecentDirectory();
            }
          } finally {
            serializationLock.unlock();
          }
        }
      } else {
        // All updates have been serialized. Simply return the most recent
        if (debug || Log.levelMet(Level.INFO)) {
          Log.warningf("c serializedVersions.get %s %d", KeyUtil.keyToString(dirKey), latestUpdateSP.getVersion());
        }
        sd = getMostRecentDirectory();
      }
    } else {
      Map.Entry<Long, SerializedDirectory> entry;

      // This retrieve() is asking for a historical version of some sort

      // For directories, versions must be ascending, max creation time not allowed in vc
      if (vc.getMode().equals(VersionConstraint.Mode.GREATEST)) {
        entry = serializedVersions.floorEntry(vc.getMax());
        if (entry != null && entry.getKey() < vc.getMin()) {
          entry = null;
        }
      } else {
        entry = serializedVersions.ceilingEntry(vc.getMin());
        if (entry != null && entry.getKey() > vc.getMax()) {
          entry = null;
        }
      }
      if (entry != null) {
        sd = entry.getValue();
      } else {
        sd = null;
      }
    }
    if (debug || Log.levelMet(Level.INFO)) {
      Log.warningf("retrieve sd %s", sd);
    }
    if (sd != null) {
      try {
        sdp = sd.readDir();

        rVal = sdp.getV2();
        if (!rVal.hasRemaining()) {
          // Shouldn't happen. Remove eventually
          Log.warningf("fixing rVal %s", rVal);
          rVal.rewind();
          Log.warningf("rVal %s", rVal);
          if (!rVal.hasRemaining()) {
            try {
              Log.warningf("rVal %s", rVal);
              throw new RuntimeException();
            } catch (RuntimeException re) {
              re.printStackTrace(System.out);
            }
          }
        }
        if (debug || Log.levelMet(Level.INFO)) {
          Log.warningf("retrieve rVal %s", rVal);
        }
        return rVal;
      } catch (IOException ioe) {
        Log.logErrorWarning(ioe);
      }
    }
    return null;
  }
}
