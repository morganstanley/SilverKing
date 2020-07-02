package com.ms.silverking.cloud.skfs.dir.serverside;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.TimeAndVersionRetentionPolicy;
import com.ms.silverking.cloud.dht.TimeAndVersionRetentionState;
import com.ms.silverking.cloud.dht.ValueRetentionPolicy;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.common.CCSSUtil;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.MetaDataUtil;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.PeerHealthIssue;
import com.ms.silverking.cloud.dht.daemon.storage.StorageParameters;
import com.ms.silverking.cloud.dht.serverside.SSNamespaceStore;
import com.ms.silverking.cloud.dht.serverside.SSRetrievalOptions;
import com.ms.silverking.cloud.dht.serverside.SSStorageParameters;
import com.ms.silverking.cloud.dht.serverside.SSUtil;
import com.ms.silverking.cloud.skfs.dir.DirectoryBase;
import com.ms.silverking.cloud.skfs.dir.DirectoryInMemory;
import com.ms.silverking.cloud.skfs.dir.DirectoryInPlace;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.compression.CompressionUtil;
import com.ms.silverking.io.FileUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.time.SimpleTimer;
import com.ms.silverking.time.Timer;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;

/**
 * Server-side base class for implementations of DirectoryInMemory
 */
public abstract class BaseDirectoryInMemorySS extends DirectoryInMemory {
  protected final DHTKey dirKey;
  protected final File sDir;
  protected final ConcurrentNavigableMap<Long, SerializedDirectory> serializedVersions;
  protected final NamespaceOptions nsOptions;
  protected SSStorageParameters latestUpdateSP;
  protected final Timer reapTimer;
  protected long lastPersistenceCheckMillis;
  private final Lock reapLock;

  protected static final int reapIntervalMinutes = 10;
  protected static final int reapMinVersions = 1;
  protected static final int reapMaxVersions = 128;

  private static final long minPersistenceIntervalMillis = 5 * 1000;

  public static String compressionProperty = BaseDirectoryInMemorySS.class.getCanonicalName() + ".Compression";
  private static final Compression defaultDimSSCompression = Compression.LZ4;
  private static final Compression dimSSCompression;
  private static final double compressionThreshold = 0.8;

  private static final boolean debug = false;
  private static final boolean debugPersistence = false;

  protected static FileDeletionWorker fileDeletionWorker;

  static {
    String compressionValue;
    Compression _compression;

    compressionValue = PropertiesHelper.systemHelper.getString(compressionProperty, UndefinedAction.ZeroOnUndefined);
    if (compressionValue == null) {
      _compression = defaultDimSSCompression;
    } else {
      try {
        _compression = Compression.valueOf(compressionValue);
      } catch (Exception e) {
        Log.logErrorWarning(e, "Using default mode " + defaultDimSSCompression);
        _compression = defaultDimSSCompression;
      }
    }
    dimSSCompression = _compression;
    Log.warningf("BaseDirectoryInMemorySS dimSSCompression %s", dimSSCompression);
  }

  BaseDirectoryInMemorySS(DHTKey dirKey, DirectoryBase d, SSStorageParameters storageParams, File sDir,
      NamespaceOptions nsOptions, boolean reap, boolean lockReaps) {
    super(d);

    boolean dirCreated;

    this.dirKey = dirKey;
    if (!sDir.exists()) {
      if (!sDir.mkdir()) {
        throw new RuntimeException("Unable to create " + sDir.getAbsolutePath());
      } else {
        dirCreated = true;
      }
    } else {
      dirCreated = false;
    }
    this.latestUpdateSP = storageParams;
    this.sDir = sDir;
    this.nsOptions = nsOptions;
    serializedVersions = new ConcurrentSkipListMap<>();
    if (lockReaps) {
      reapLock = new ReentrantLock();
    } else {
      reapLock = null;
    }
    if (!dirCreated) {
      recover();
      if (reap) {
        DirectoryServer.fileDeletionWorker.delete(reap());
      }
    }
    reapTimer = new SimpleTimer(TimeUnit.MINUTES, reapIntervalMinutes);
  }

  private void recover() {
    List<Long> versions;

    Log.warningf("Recovering directory from %s", sDir);
    versions = FileUtil.numericFilesInDirAsSortedLongList(sDir);
    for (long version : versions) {
      try {
        Pair<SSStorageParameters, byte[]> sd;
        SerializedDirectory _sd;
        DirectoryInPlace recoveredDir;
        byte[] b;
        int _dataOffset;

        sd = readFromDisk(version);
        _sd = new SerializedDirectory(sd, true);
        serializedVersions.put(version, _sd);
        latestUpdateSP = _sd.getStorageParameters();
        _dataOffset = MetaDataUtil.getDataOffset(sd.getV1().getChecksumType());
        if (sd.getV1().getCompression() != Compression.NONE) {
          b = CompressionUtil.decompress(sd.getV1().getCompression(), sd.getV2(), _dataOffset,
              sd.getV2().length - _dataOffset, sd.getV1().getUncompressedSize());
          recoveredDir = new DirectoryInPlace(b, 0, b.length);
        } else {
          b = sd.getV2();
          recoveredDir = new DirectoryInPlace(b, _dataOffset, b.length);
        }
        //recoveredDir.display();
        super.update(recoveredDir);
        //Log.warningf("Recovered version %d", version);
      } catch (IOException ioe) {
        Log.logErrorWarning(ioe, "Failed to recover " + sDir + " " + version);
      }
    }
  }

  protected List<File> reap() {
    List<File> filesToRemove;

    filesToRemove = ImmutableList.of();
    if (reapLock != null) {
      reapLock.lock();
    }
    try {
      // Note: size() is linear for ConcurrentSkipListMap
      if (serializedVersions.size() > reapMinVersions) {
        ValueRetentionPolicy vrp;
        Set<Long> versionsToRemove;
        long greatestVersion;

        greatestVersion = Long.MIN_VALUE;
        if (Log.levelMet(Level.INFO)) {
          Log.infof("Reap DirectoryInMemorySS %s", KeyUtil.keyToString(dirKey));
        }
        versionsToRemove = new HashSet<>();
        vrp = nsOptions.getValueRetentionPolicy();
        if (vrp instanceof TimeAndVersionRetentionPolicy) {
          TimeAndVersionRetentionState rs;
          long curTimeNanos;

          rs = new TimeAndVersionRetentionState();
          curTimeNanos = SystemTimeUtil.skSystemTimeSource.absTimeNanos();
          for (long version : serializedVersions.descendingKeySet()) {
            if (greatestVersion == Long.MIN_VALUE) {
              greatestVersion = version;
            }
            if (!vrp.retains(dirKey, version, curTimeNanos, false, rs, curTimeNanos, 0)) {
              versionsToRemove.add(version);
            }
          }
        } else {
          Log.warning("Unexpected retention policy: " + vrp);
        }
        if (greatestVersion != Long.MIN_VALUE) {
          ensurePersisted(greatestVersion);
        }
        filesToRemove = remove(versionsToRemove);
      }
    } finally {
      if (reapLock != null) {
        reapLock.unlock();
      }
    }
    return filesToRemove;
  }

  private void ensurePersisted(long version) {
    SerializedDirectory sd;

    sd = serializedVersions.get(version);
    if (sd != null && !sd.isPersisted()) {
      persist(sd);
    }
  }

  private List<File> remove(Set<Long> versionsToRemove) {
    List<File> filesToRemoveLater;

    filesToRemoveLater = new ArrayList<>();
    for (long version : versionsToRemove) {
      SerializedDirectory sd;

      //Log.warningf("Removing %s %d", dirKey, version);
      sd = serializedVersions.remove(version);
      if (sd != null) {
        if (sd.isPersisted()) {
          File f;

          f = removeFromDisk(version);
          if (f != null) {
            filesToRemoveLater.add(f);
          }
        }
      }
    }
    return filesToRemoveLater;
  }

  ///////////////////////////////////////////////////

  protected Pair<SSStorageParameters, byte[]> serializeDir() {
    byte[] serializedDir;
    byte[] compressedDir;
    StorageParameters sp;
    Compression compressionUsed;

    serializedDir = serialize();
    compressionUsed = Compression.NONE;
    compressedDir = serializedDir;
    if (dimSSCompression != Compression.NONE) {
      try {
        compressedDir = CompressionUtil.compress(dimSSCompression, serializedDir, 0, serializedDir.length);
        if ((double) compressedDir.length / (double) serializedDir.length < compressionThreshold) {
          compressionUsed = dimSSCompression;
        } else {
          compressedDir = serializedDir;
        }
      } catch (IOException ioe) {
        Log.logErrorWarning(ioe);
      }
    }
    sp = createDirMetaData(serializedDir.length, compressedDir.length, compressionUsed);
    return new Pair<>(sp, SSUtil.rawValueToStoredValue(compressedDir, sp));
  }

  protected StorageParameters serializeDirMetaData() {
    // Note, this version of the meta data is for uncompressed
    // After serialization has taken place, the actual data may be compressed
    return createDirMetaData(computeSerializedSize());
  }

  private StorageParameters createDirMetaData(int serializedDirSize) {
    return createDirMetaData(serializedDirSize, serializedDirSize, Compression.NONE);
  }

  private StorageParameters createDirMetaData(int serializedDirSize, int compressedDirSize, Compression compression) {
    StorageParameters sp;
    byte[] checksum;

    checksum = new byte[0];
    sp = new StorageParameters(latestUpdateSP.getVersion(), serializedDirSize, compressedDirSize,
        latestUpdateSP.getLockSeconds(), CCSSUtil.createCCSS(compression, ChecksumType.NONE), checksum,
        latestUpdateSP.getValueCreator(), latestUpdateSP.getCreationTime());
    return sp;
  }

  ///////////////////////////////////////////////////

  /**
   * Only called from Persister
   * - update() calls may be in progress
   * - retrieve() calls may be in progress
   *
   * @param checkTimeMillis
   * @param nsStore
   * @return
   */
  public List<File> checkForPersistence(long checkTimeMillis, SSNamespaceStore nsStore) {
    List<File> filesToDelete;

    if (debugPersistence || Log.levelMet(Level.INFO)) {
      Log.warningf("checkForPersistence %s", KeyUtil.keyToString(dirKey));
    }

    filesToDelete = ImmutableList.of();
    // could use a timer below, but should avoid clock checking overhead
    if (debugPersistence) {
      Log.warningf("%d %d %d\t%d", checkTimeMillis, lastPersistenceCheckMillis, minPersistenceIntervalMillis,
          checkTimeMillis - lastPersistenceCheckMillis);
    }
    if (checkTimeMillis - lastPersistenceCheckMillis > minPersistenceIntervalMillis) {
      lastPersistenceCheckMillis = checkTimeMillis;
      persistLatestIfNecessary(nsStore);
      filesToDelete = reap();
    }
    if (debugPersistence || Log.levelMet(Level.INFO)) {
      Log.warningf("out checkForPersistence %s", KeyUtil.keyToString(dirKey));
    }
    return filesToDelete;
  }

  protected void persistLatestIfNecessary(SSNamespaceStore nsStore) {
    Map.Entry<Long, SerializedDirectory> entry;

    if (debugPersistence || Log.levelMet(Level.INFO)) {
      Log.warningf("persistLatestIfNecessary() %s", KeyUtil.keyToString(dirKey));
    }
    entry = serializedVersions.lastEntry();
    if (entry != null) {
      SerializedDirectory sd;

      if (debugPersistence || Log.levelMet(Level.INFO)) {
        Log.warning("persistLatestIfNecessary() found entry %s", KeyUtil.keyToString(dirKey));
      }
      sd = entry.getValue();
      if (debugPersistence || Log.levelMet(Level.INFO)) {
        Log.warningf("persistLatestIfNecessary() entry persisted %s %s", KeyUtil.keyToString(dirKey), sd.isPersisted());
      }
      if (!sd.isPersisted()) {
        persist(sd);
      }
    } else {
      if (debugPersistence || Log.levelMet(Level.INFO)) {
        Log.warningf("persistLatestIfNecessary() no lastEntry");
      }
    }
  }

  protected final void persist(SerializedDirectory sd) {
    if (debugPersistence) {
      Log.warningf("persist()");
    }
    synchronized (sd) {
      if (!sd.isPersisted()) {
    persist(sd.getStorageParameters(), sd.getSerializedDir());
    sd.setPersisted();
  }
    }
  }

  private final void persist(SSStorageParameters sp, byte[] serializedDirData) {
    try {
      writeToDisk(sp, serializedDirData);
    } catch (IOException e) {
      peerHealthMonitor.addSelfAsSuspect(PeerHealthIssue.StorageError);
      Log.logErrorWarning(e);
    }
  }

  private File removeFromDisk(long version) {
    if (fileDeletionWorker != null) {
      fileDeletionWorker.delete(fileForVersion(version));
      return null;
    } else {
      // delete later as it may be in use by retrieve
      return fileForVersion(version);
    }
  }

  private void writeToDisk(SSStorageParameters sp, byte[] serializedDirData) throws IOException {
    if (Log.levelMet(Level.INFO)) {
      Log.infof("persisting %s", fileForVersion(sp));
    }
    FileUtil.writeToFile(fileForVersion(sp), serializedDirData);
  }

  Pair<SSStorageParameters, byte[]> readFromDisk(long version) throws IOException {
    byte[] b;
    SSStorageParameters sp;

    b = FileUtil.readFileAsBytes(fileForVersion(version));
    sp = StorageParameterSerializer.deserialize(b);
    return new Pair<>(sp, b);
  }

  Pair<SSStorageParameters, ByteBuffer> readFromDisk_Mapped(long version) throws IOException {
    ByteBuffer b;
    SSStorageParameters sp;

    b = FileUtil.mapFile(fileForVersion(version), FileUtil.FileMapMode.FileBackedMap_ReadOnly);
    sp = StorageParameterSerializer.deserialize(b); // deserialize will duplicate the buffer for safety
    if (debug || Log.levelMet(Level.INFO)) {
      Log.warningf("readFromDisk_Mapped b %s", b);
    }
    return new Pair<>(sp, b);
  }

  private File fileForVersion(SSStorageParameters sp) {
    return fileForVersion(sp.getVersion());
  }

  private File fileForVersion(long version) {
    return new File(sDir, Long.toString(version));
  }

  protected class SerializedDirectory {
    private final SSStorageParameters sp;
    private ByteBuffer serializedDir;
    private boolean isPersisted;

    public SerializedDirectory(SSStorageParameters sp, ByteBuffer serializedDir, boolean isPersisted) {
      this.sp = sp;
      if (serializedDir != null) {
        this.serializedDir = serializedDir;
      } else {
        this.serializedDir = ByteBuffer.allocate(
            sp.getUncompressedSize()); // For now, create dummy value; future remove
      }
      this.isPersisted = isPersisted;
    }

    public SerializedDirectory(SSStorageParameters sp, byte[] serializedDir, boolean isPersisted) {
      this(sp, ByteBuffer.wrap(serializedDir), isPersisted);
    }

    public SerializedDirectory(Pair<SSStorageParameters, byte[]> sd, boolean isPersisted) {
      this(sd.getV1(), ByteBuffer.wrap(sd.getV2()), isPersisted);
    }

    public SSStorageParameters getStorageParameters() {
      return sp;
    }

    public byte[] getSerializedDir() {
      return serializedDir.array();
    }

    public void clearCachedData() {
      this.serializedDir = null;
    }

    public Pair<SSStorageParameters, ByteBuffer> readDir() throws IOException {
      if (serializedDir != null) {
        return new Pair<>(sp, serializedDir.duplicate());
      } else {
        return readFromDisk_Mapped(sp.getVersion());
      }
    }

    public void setPersisted() {
      this.isPersisted = true;
      serializedDir = null;
    }

    public boolean isPersisted() {
      return isPersisted;
    }

    @Override
    public String toString() {
      return String.format("%s:%s:%s", sp, serializedDir != null ? serializedDir.remaining() : "<null>", isPersisted);
    }
  }

  ///////////////////////////////////////////////////

  protected SerializedDirectory getMostRecentDirectory() {
    Map.Entry<Long, SerializedDirectory> entry;

    entry = serializedVersions.lastEntry();
    if (entry == null) {
      Log.info("getMostRecentDirectory() no entries");
      return null;
    } else {
      Log.info("getMostRecentDirectory() %s", entry.getValue());
      return entry.getValue();
    }
  }

  public abstract void update(DirectoryBase update, SSStorageParameters sp);

  public abstract ByteBuffer retrieve(SSRetrievalOptions options);
}
