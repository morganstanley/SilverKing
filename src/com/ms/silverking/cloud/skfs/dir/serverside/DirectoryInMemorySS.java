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
	
	private static final int	reapIntervalMinutes = 10;
	private static final int	reapMinVersions = 1;
	private static final int	reapMaxVersions = 32;
	
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
		if (!dirCreated && reap) {
			recover();
			reap();
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
				_sd = new SerializedDirectory(sd);
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
			
			Log.warningf("Reap DirectoryInMemorySS %s", KeyUtil.keyToString(dirKey));
			versionsToRemove = new HashSet<>();
			vrp = nsOptions.getValueRetentionPolicy();
			if (vrp instanceof TimeAndVersionRetentionPolicy) {
				TimeAndVersionRetentionState	rs;
				long	curTimeNanos;
				
				rs = new TimeAndVersionRetentionState();
				curTimeNanos = SystemTimeSource.instance.absTimeNanos();
				for (long version : serializedVersions.descendingKeySet()) {
					if (!vrp.retains(dirKey, version, curTimeNanos, false, rs, curTimeNanos)) {
						versionsToRemove.add(version);
					}
				}
			} else {
				Log.warning("Unexpected retention policy: "+ vrp);
			}
			remove(versionsToRemove);
		}
	}
	
	private void remove(Set<Long> versionsToRemove) {
		for (long version : versionsToRemove) {
			//Log.warningf("Removing %s %d", dirKey, version);
			serializedVersions.remove(version);
			removeFromDisk(version);
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
		persist(sd.getV1(), sd.getV2());
		serializedVersions.put(sp.getVersion(), new SerializedDirectory(sd));
		if (reapTimer.hasExpired() || serializedVersions.size() > reapMaxVersions) {
			reap();
			reapTimer.reset();
		}
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
		
		public SerializedDirectory(SSStorageParameters sp, byte[] serializedDir) {
			this.sp = sp;
			this.serializedDir = serializedDir;
		}
		
		public SerializedDirectory(Pair<SSStorageParameters,byte[]> sd) {
			this(sd.getV1(), sd.getV2());
		}
		
		public SSStorageParameters getStorageParameters() {
			return sp;
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
				
				rVal = SSUtil.retrievalResultBufferFromValue(sdp.getV2(), StorageParameters.fromSSStorageParameters(sdp.getV1()));
				return rVal;
			} catch (IOException ioe) {
				Log.logErrorWarning(ioe);
			}
		}
		return null;
	}	
}
