package com.ms.silverking.cloud.skfs.dir.serverside;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.serverside.PutTrigger;
import com.ms.silverking.cloud.dht.serverside.RetrieveTrigger;
import com.ms.silverking.cloud.dht.serverside.SSNamespaceStore;
import com.ms.silverking.cloud.dht.serverside.SSRetrievalOptions;
import com.ms.silverking.cloud.dht.serverside.SSStorageParameters;
import com.ms.silverking.cloud.skfs.dir.DirectoryInPlace;
import com.ms.silverking.compression.CompressionUtil;
import com.ms.silverking.log.Log;

public class DirectoryServer implements PutTrigger, RetrieveTrigger {
	private final ConcurrentMap<DHTKey, DirectoryInMemorySS>	directories;
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
	}
	
	@Override
	public OpResult put(SSNamespaceStore nsStore, DHTKey key, ByteBuffer value, SSStorageParameters storageParams, byte[] userData,
			NamespaceVersionMode nsVersionMode) {
		byte[]				buf;
		int					bufOffset;
		int					bufLimit;
		DirectoryInMemorySS	existingDir;
		DirectoryInPlace	updateDir;
		
		//Log.warningf("DirectoryServer.put() %s %s %s  %s %s", KeyUtil.keyToString(key), stringToKeyString("/"), stringToKeyString("/skfs"), value.hasArray(), storageParams.getCompression());

		buf = value.array();
		bufOffset = value.arrayOffset();
		bufLimit = bufOffset + value.limit();
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
}
