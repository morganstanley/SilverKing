package com.ms.silverking.cloud.skfs.dir.serverside;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.serverside.PutTrigger;
import com.ms.silverking.cloud.dht.serverside.RetrieveTrigger;
import com.ms.silverking.cloud.dht.serverside.SSNamespaceStore;
import com.ms.silverking.cloud.dht.serverside.SSRetrievalOptions;
import com.ms.silverking.cloud.dht.serverside.SSStorageParameters;
import com.ms.silverking.cloud.skfs.dir.DirectoryInPlace;
import com.ms.silverking.compression.CompressionUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.process.SafeThread;
import com.ms.silverking.text.StringUtil;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;

public class DirectoryServer implements PutTrigger, RetrieveTrigger {
	private SSNamespaceStore nsStore;
	private final ConcurrentMap<DHTKey, BaseDirectoryInMemorySS>	directories;
	private Set<DHTKey>	directoriesOnDiskAtBoot;
	private File	logDir;
	private NamespaceOptions nsOptions;
	
	private enum Mode {Eager, Lazy};
	
	public static String	modeProperty = DirectoryServer.class.getCanonicalName() +".Mode";
	private static final Mode	defaultMode = Mode.Lazy;
	private static final Mode	mode;
	
	private static final boolean	debug = false;
	
	static FileDeletionWorker	fileDeletionWorker = new FileDeletionWorker();
	
	static {
		String	modeValue;
		Mode	_mode;
		
		Log.warning("Initialized DirectoryServer class");
		modeValue = PropertiesHelper.systemHelper.getString(modeProperty, UndefinedAction.ZeroOnUndefined);
		if (modeValue == null) {
			_mode = defaultMode;
		} else {
			try {
				_mode = Mode.valueOf(modeValue);
			} catch (Exception e) {
				Log.logErrorWarning(e, "Using default mode "+ defaultMode);
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
		Set<DHTKey>	dirs;
		
		dirs = new HashSet<>();
		for (String s : logDir.list()) {
			try {
				dirs.add(KeyUtil.keyStringToKey(s));
			} catch (Exception e) {
				Log.warningf("DirectoryServer.getDirectoriesOnDiskAtBoot() skipping %s", s);
			}
		}
		return ImmutableSet.copyOf(dirs);
	}
	
	private BaseDirectoryInMemorySS newDirectoryInMemorySS(DHTKey dirKey, DirectoryInPlace d, SSStorageParameters storageParams, File sDir, NamespaceOptions nsOptions) {
		return newDirectoryInMemorySS(dirKey, d, storageParams, sDir, nsOptions, true);
	}
	
	private BaseDirectoryInMemorySS newDirectoryInMemorySS(DHTKey dirKey, DirectoryInPlace d, SSStorageParameters storageParams, File sDir, NamespaceOptions nsOptions, boolean reap) {
		if (mode == Mode.Lazy) {
			return new LazyDirectoryInMemorySS(dirKey, d, storageParams, sDir, nsOptions, reap);
		} else {
			return new EagerDirectoryInMemorySS(dirKey, d, storageParams, sDir, nsOptions, reap);
		}
	}

	@Override
	public OpResult put(SSNamespaceStore nsStore, DHTKey key, ByteBuffer value, SSStorageParameters storageParams, byte[] userData,
			NamespaceVersionMode nsVersionMode) {
		//Stopwatch	sw;
		
		//sw = new SimpleStopwatch();
		//try {
			byte[]				buf;
			int					bufOffset;
			int					bufLimit;
			BaseDirectoryInMemorySS	existingDir;
			DirectoryInPlace	updateDir;
			
			if (debug || Log.levelMet(Level.INFO)) {
				Log.warningf("DirectoryServer.put() %s %s %s", KeyUtil.keyToString(key), value.hasArray(), storageParams.getCompression());
			}
	
			buf = value.array();
			bufOffset = value.arrayOffset() + value.position();
			bufLimit = value.limit();
			//System.out.printf("%s\n", value);
			//System.out.printf("%d %d %d\n", buf.length, bufOffset, bufLimit);
			if (storageParams.getCompression() == Compression.NONE) {
				//System.out.printf("No compression\n");
			} else {
				try {
					int	dataOffset;
					
					//System.out.printf("Compression\n");
					dataOffset = value.position();
					buf = CompressionUtil.decompress(storageParams.getCompression(), value.array(), dataOffset, storageParams.getCompressedSize(), storageParams.getUncompressedSize());
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
			existingDir = getExistingDirectory(key, true);
			if (existingDir == null) {
				BaseDirectoryInMemorySS	newDir;
				
				newDir = newDirectoryInMemorySS(key, updateDir, storageParams, new File(logDir, KeyUtil.keyToString(key)), nsStore.getNamespaceOptions());
				newDir.update(updateDir, storageParams);
				directories.put(key, newDir);
			} else {
				existingDir.update(updateDir, storageParams);
			}
			return OpResult.SUCCEEDED;
		//} finally {
		//	sw.stop();
		//	Log.warningf("DirectoryServer.put()\t%f", sw.getElapsedSeconds());
		//}
	}
	

	@Override
	public OpResult putUpdate(SSNamespaceStore nsStore, DHTKey key, long version, byte storageState) {
		return OpResult.SUCCEEDED;
	}
	
	@Override
	public ByteBuffer retrieve(SSNamespaceStore nsStore, DHTKey key, SSRetrievalOptions options) {
		BaseDirectoryInMemorySS	existingDir;
		ByteBuffer	rVal;
		
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
	
	/**
	 * Check to see if given directory is already in memory. If not,
	 * check to see if it exists on disk; if so, then read it in to memory. 
	 * @param key
	 * @return the given DirectoryInMemorySS if it exists in memory or was found on disk
	 */
	public BaseDirectoryInMemorySS getExistingDirectory(DHTKey key, boolean reapOnRecover) {
		BaseDirectoryInMemorySS	existingDir;
		
		existingDir = directories.get(key);
		if (existingDir == null) {
			File	dir;
			
			dir = new File(logDir, KeyUtil.keyToString(key));
			if (dir.exists()) {
				BaseDirectoryInMemorySS	prev;
				
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
		Set<DHTKey>	keys;
		
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
		private final Thread	pThread;
		
		private static final long	checkIntervalMillis = 1 * 1000;
		
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
			long	checkTimeMillis;
			List<File>	filesToRemove;
			
			Log.info("checkForPersistence()");
			filesToRemove = new ArrayList<>();
			checkTimeMillis = SystemTimeUtil.systemTimeSource.absTimeMillis();
			for (BaseDirectoryInMemorySS dir : directories.values()) {
				List<File>	dirFilesToRemove;
				
				dirFilesToRemove = dir.checkForPersistence(checkTimeMillis);
				filesToRemove.addAll(dirFilesToRemove);
			}
			deleteFiles(filesToRemove);
			Log.info("out checkForPersistence()");
		}
		
		private void deleteFiles(List<File>	filesToRemove) {
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
}
