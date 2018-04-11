package com.ms.silverking.cloud.skfs.dir.serverside;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.TimeAndVersionRetentionPolicy;
import com.ms.silverking.cloud.dht.TimeAndVersionRetentionState;
import com.ms.silverking.cloud.dht.ValueRetentionPolicy;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.common.CCSSUtil;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.daemon.storage.StorageParameters;
import com.ms.silverking.cloud.dht.serverside.SSRetrievalOptions;
import com.ms.silverking.cloud.dht.serverside.SSStorageParameters;
import com.ms.silverking.cloud.dht.serverside.SSUtil;
import com.ms.silverking.cloud.skfs.dir.DirectoryInMemory;
import com.ms.silverking.cloud.skfs.dir.DirectoryInPlace;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.io.FileUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.time.SimpleTimer;
import com.ms.silverking.time.SystemTimeSource;
import com.ms.silverking.time.Timer;

/**
 * Extends DirectoryInPlace to store StorageParameters
 */
public class DirectoryInMemorySS extends DirectoryInMemory {
	private final DHTKey			dirKey;
	private final File				sDir;
	private final NavigableMap<Long,SerializedDirectory>	serializedVersions;
	private final NamespaceOptions	nsOptions;
	private SSStorageParameters	latestUpdateSP;
	private final Timer			reapTimer;
	private long			lastPersistenceCheckMillis;
	
	private static final int	reapIntervalMinutes = 10;
	private static final int	reapMinVersions = 1;
	private static final int	reapMaxVersions = 128;
	
	private static final long	minPersistenceIntervalMillis = 5 * 1000;
	
	private static final FileDeletionWorker	fileDeletionWorker = new FileDeletionWorker();

	DirectoryInMemorySS(DHTKey dirKey, DirectoryInPlace d, SSStorageParameters storageParams, File sDir, NamespaceOptions nsOptions, boolean reap) {
		super(d);
		
		boolean	dirCreated;
		
		this.dirKey = dirKey;
		if (!sDir.exists()) {
			if (!sDir.mkdir()) {
				throw new RuntimeException("Unable to create "+ sDir.getAbsolutePath());
			} else {
				dirCreated = true;
			}
		} else {
			dirCreated = false;
		}
		this.latestUpdateSP = storageParams;
		this.sDir = sDir;
		this.nsOptions = nsOptions;
		serializedVersions = new TreeMap<>();
		if (!dirCreated) {
			recover();
			if (reap) {
				reap();
			}
		}
		reapTimer = new SimpleTimer(TimeUnit.MINUTES, reapIntervalMinutes);
	}
	
	public DirectoryInMemorySS(DHTKey dirKey, DirectoryInPlace d, SSStorageParameters storageParams, File sDir, NamespaceOptions nsOptions) {
		this(dirKey, d, storageParams, sDir, nsOptions, true);
	}
	
	private void recover() {
		List<Long>		versions;
		
		Log.warningf("Recovering directory from %s", sDir);
		versions = FileUtil.numericFilesInDirAsSortedLongList(sDir);
		for (long version : versions) {
			try {
				Pair<SSStorageParameters, byte[]>	sd;
				SerializedDirectory	_sd;
				DirectoryInPlace	recoveredDir;
				
				sd = readFromDisk(version);
				_sd = new SerializedDirectory(sd, true);
				serializedVersions.put(version, _sd);
				latestUpdateSP = _sd.getStorageParameters();
				recoveredDir = new DirectoryInPlace(sd.getV2(), 0, sd.getV2().length);
				//recoveredDir.display();
				super.update(recoveredDir);
				//Log.warningf("Recovered version %d", version);
			} catch (IOException ioe) {
				Log.logErrorWarning(ioe, "Failed to recover "+ sDir +" "+ version);
			}
		}
	}
	
	private void reap() {
		if (serializedVersions.size() > reapMinVersions) {
			ValueRetentionPolicy	vrp;
			Set<Long>				versionsToRemove;
			long					greatestVersion;
			
			greatestVersion = Long.MIN_VALUE;
			if (Log.levelMet(Level.INFO)) {
				Log.infof("Reap DirectoryInMemorySS %s", KeyUtil.keyToString(dirKey));
			}
			versionsToRemove = new HashSet<>();
			vrp = nsOptions.getValueRetentionPolicy();
			if (vrp instanceof TimeAndVersionRetentionPolicy) {
				TimeAndVersionRetentionState	rs;
				long	curTimeNanos;
				
				rs = new TimeAndVersionRetentionState();
				curTimeNanos = SystemTimeSource.instance.absTimeNanos();
				for (long version : serializedVersions.descendingKeySet()) {
					if (greatestVersion == Long.MIN_VALUE) {
						greatestVersion = version;
					}
					if (!vrp.retains(dirKey, version, curTimeNanos, false, rs, curTimeNanos)) {
						versionsToRemove.add(version);
					}
				}
			} else {
				Log.warning("Unexpected retention policy: "+ vrp);
			}
			if (greatestVersion != Long.MIN_VALUE) {
				ensurePersisted(greatestVersion);
			}
			remove(versionsToRemove);
		}
	}
	
	private void ensurePersisted(long version) {
		SerializedDirectory	sd;
		
		sd = serializedVersions.get(version);
		if (sd != null && !sd.isPersisted()) {
			persist(sd);
		}
	}
	
	private void remove(Set<Long> versionsToRemove) {
		for (long version : versionsToRemove) {
			SerializedDirectory	sd;
			
			//Log.warningf("Removing %s %d", dirKey, version);
			sd = serializedVersions.remove(version);
			if (sd != null) {
				if (sd.isPersisted()) {
					removeFromDisk(version);
				}
			}
		}
	}

	/*
	public DirectoryInMemorySS(byte[] buf, int offset, int limit, SSStorageParameters storageParams) {
		super(buf, offset, limit);
		this.storageParams = storageParams;
	}
	*/
	
	public Pair<SSStorageParameters,byte[]> serializeDir() {
		byte[]	serializedDir;
		StorageParameters	sp;
		byte[]	checksum;
		
		serializedDir = serialize();
		checksum = new byte[0];
		sp = new StorageParameters(latestUpdateSP.getVersion(), serializedDir.length, serializedDir.length, 
				CCSSUtil.createCCSS(Compression.NONE, ChecksumType.NONE), checksum, latestUpdateSP.getValueCreator(), latestUpdateSP.getCreationTime());
		return new Pair<>(sp, serializedDir);
	}
	
	public void update(DirectoryInPlace update, SSStorageParameters sp) {
		Pair<SSStorageParameters,byte[]>	sd;
		
		this.latestUpdateSP = sp;
		update(update);
		sd = serializeDir();
		//persist(sd.getV1(), sd.getV2());
		serializedVersions.put(sp.getVersion(), new SerializedDirectory(sd, false));
		if (reapTimer.hasExpired() || serializedVersions.size() > reapMaxVersions) {
			reap();
			reapTimer.reset();
		}
	}
	
	public void checkForPersistence(long checkTimeMillis) {
		if (Log.levelMet(Level.INFO)) {
			Log.warningf("checkForPersistence %s", KeyUtil.keyToString(dirKey));
		}
		if (checkTimeMillis - lastPersistenceCheckMillis > minPersistenceIntervalMillis) {
			lastPersistenceCheckMillis = checkTimeMillis;
			persistLatestIfNecessary();
		}
		if (Log.levelMet(Level.INFO)) {
			Log.warningf("out checkForPersistence %s", KeyUtil.keyToString(dirKey));
		}
	}
	
	private final void persistLatestIfNecessary() {
		Map.Entry<Long, SerializedDirectory>	entry;
		
		if (Log.levelMet(Level.INFO)) {
			Log.warningf("persistLatestIfNecessary() %s", KeyUtil.keyToString(dirKey));
		}
		entry = serializedVersions.lastEntry();
		if (entry != null) {
			SerializedDirectory	sd;
			
			if (Log.levelMet(Level.INFO)) {
				Log.warning("persistLatestIfNecessary() found entry %s", KeyUtil.keyToString(dirKey));
			}
			sd = entry.getValue();
			if (Log.levelMet(Level.INFO)) {
				Log.warningf("persistLatestIfNecessary() entry persisted %s %s", KeyUtil.keyToString(dirKey), sd.isPersisted());
			}
			if (!sd.isPersisted()) {
				persist(sd);
			}
		}
	}
	
	private final void persist(SerializedDirectory sd) {
		persist(sd.getStorageParameters(), sd.getSerializedDir());
		sd.setPersisted();
	}
	
	private final void persist(SSStorageParameters sp, byte[] serializedDirData) {
		try {
			writeToDisk(sp, serializedDirData);
		} catch (IOException e) {
			Log.logErrorWarning(e);
		}
	}
	
	private void removeFromDisk(long version) {
		fileDeletionWorker.delete(fileForVersion(version));
	}
	
	private void writeToDisk(SSStorageParameters sp, byte[] serializedDirData) throws IOException {
		if (Log.levelMet(Level.INFO)) {
			Log.infof("persisting %s", fileForVersion(sp));
		}
		FileUtil.writeToFile(fileForVersion(sp), StorageParameterSerializer.serialize(sp), serializedDirData);
	}
	
	Pair<SSStorageParameters, byte[]> readFromDisk(long version) throws IOException {
		byte[]	b;
		SSStorageParameters	sp;
		byte[]	serializedDir;
		int	spLength;
		
		b = FileUtil.readFileAsBytes(fileForVersion(version));
		sp = StorageParameterSerializer.deserialize(b);
		spLength = StorageParameterSerializer.getSerializedLength(sp);
		serializedDir = new byte[b.length - spLength];
		System.arraycopy(b, spLength, serializedDir, 0, b.length - spLength);
		return new Pair<>(sp, serializedDir);
	}
	
	private File fileForVersion(SSStorageParameters sp) {
		return fileForVersion(sp.getVersion());
	}
	
	private File fileForVersion(long version) {
		return new File(sDir, Long.toString(version));
	}
	
	class SerializedDirectory {
		private final SSStorageParameters	sp;
		private byte[]	serializedDir;
		private boolean	isPersisted;
		
		public SerializedDirectory(SSStorageParameters sp, byte[] serializedDir, boolean isPersisted) {
			this.sp = sp;
			this.serializedDir = serializedDir;
			this.isPersisted = isPersisted;
		}
		
		public SerializedDirectory(Pair<SSStorageParameters,byte[]> sd, boolean isPersisted) {
			this(sd.getV1(), sd.getV2(), isPersisted);
		}
		
		public SSStorageParameters getStorageParameters() {
			return sp;
		}
		
		public byte[] getSerializedDir() {
			return serializedDir;
		}
		
		public void clearCachedData() {
			this.serializedDir = null;
		}
		
		public Pair<SSStorageParameters,byte[]> readDir() throws IOException {
			if (serializedDir != null) {
				return new Pair<>(sp, serializedDir);
			} else {
				return readFromDisk(sp.getVersion());
			}
		}
		
		public void setPersisted() {
			this.isPersisted = true;
		}
		
		public boolean isPersisted() {
			return isPersisted;
		}
	}
	
	public ByteBuffer retrieve(SSRetrievalOptions options) {
		ByteBuffer	rVal;
		VersionConstraint	vc;
		SerializedDirectory	sd;
		Pair<SSStorageParameters,byte[]>	sdp;
		
		vc = options.getVersionConstraint();
		if (vc.equals(VersionConstraint.greatest)) {
			sd = serializedVersions.get(latestUpdateSP.getVersion());
		} else {
			Map.Entry<Long,SerializedDirectory>	entry;
			
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
		if (sd != null) {
			try {
				sdp = sd.readDir();
				
				rVal = SSUtil.retrievalResultBufferFromValue(sdp.getV2(), StorageParameters.fromSSStorageParameters(sdp.getV1()), options);
				return rVal;
			} catch (IOException ioe) {
				Log.logErrorWarning(ioe);
			}
		}
		return null;
	}	
}
