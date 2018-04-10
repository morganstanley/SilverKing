package com.ms.silverking.cloud.skfs.dir.serverside;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
import com.ms.silverking.thread.ThreadUtil;

public class DirectoryServer implements PutTrigger, RetrieveTrigger {
	private final ConcurrentMap<DHTKey, DirectoryInMemorySS>	directories;
	private Set<DHTKey>	directoriesOnDiskAtBoot;
	private File	logDir;
	private NamespaceOptions nsOptions;
	
	static {
		Log.warning("Initialized DirectoryServer class");
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

	@Override
	public OpResult put(SSNamespaceStore nsStore, DHTKey key, ByteBuffer value, SSStorageParameters storageParams, byte[] userData,
			NamespaceVersionMode nsVersionMode) {
		byte[]				buf;
		int					bufOffset;
		int					bufLimit;
		DirectoryInMemorySS	existingDir;
		DirectoryInPlace	updateDir;
		
		//Log.warningf("DirectoryServer.put() %s %s %s", KeyUtil.keyToString(key), value.hasArray(), storageParams.getCompression());

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
		existingDir = getExistingDirectory(key);
		if (existingDir == null) {
			DirectoryInMemorySS	newDir;
			
			newDir = new DirectoryInMemorySS(key, updateDir, storageParams, new File(logDir, KeyUtil.keyToString(key)), nsStore.getNamespaceOptions());
			newDir.update(updateDir, storageParams);
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
	public ByteBuffer retrieve(SSNamespaceStore nsStore, DHTKey key, SSRetrievalOptions options) {
		DirectoryInMemorySS	existingDir;
		ByteBuffer	rVal;
		
		//Log.warningf("retrieve %s", KeyUtil.keyToString(key));
		existingDir = getExistingDirectory(key);
		if (existingDir != null) {
			//Log.warning("existingDir found");
			rVal = existingDir.retrieve(options);
		} else {
			//Log.warning("existingDir not found");
			rVal = null;
		}
		return rVal;
	}
	
	/**
	 * Check to see if given directory is already in memory. If not,
	 * check to see if it exists on disk; if so, then read it in to memory. 
	 * @param key
	 * @return the given DirectoryInMemorySS if it exists in memory or was found on disk
	 */
	public DirectoryInMemorySS getExistingDirectory(DHTKey key) {
		DirectoryInMemorySS	existingDir;
		
		existingDir = directories.get(key);
		if (existingDir == null) {
			File	dir;
			
			dir = new File(logDir, KeyUtil.keyToString(key));
			if (dir.exists()) {
				DirectoryInMemorySS	prev;
				
				Log.warningf("DirectoryServer.getExistingDirectory() recovering %s", KeyUtil.keyToString(key));
				existingDir = new DirectoryInMemorySS(key, null, null, dir, nsOptions);
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
			
			Log.info("checkForPersistence()");
			checkTimeMillis = SystemTimeUtil.systemTimeSource.absTimeMillis();
			for (DirectoryInMemorySS dir : directories.values()) {
				dir.checkForPersistence(checkTimeMillis);
			}
			Log.info("out checkForPersistence()");
		}
	}
}
