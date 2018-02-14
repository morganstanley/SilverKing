package com.ms.silverking.cloud.skfs.dir.serverside;

import com.ms.silverking.cloud.dht.serverside.SSStorageParameters;
import com.ms.silverking.cloud.skfs.dir.DirectoryInMemory;
import com.ms.silverking.cloud.skfs.dir.DirectoryInPlace;

/**
 * Extends DirectoryInPlace to store StorageParameters
 */
public class DirectoryInMemorySS extends DirectoryInMemory {
	private SSStorageParameters	storageParams;
	
	public DirectoryInMemorySS(DirectoryInPlace d, SSStorageParameters storageParams) {
		super(d);
		this.storageParams = storageParams;
	}

	public DirectoryInMemorySS(byte[] buf, int offset, int limit, SSStorageParameters storageParams) {
		super(buf, offset, limit);
		this.storageParams = storageParams;
	}

	public SSStorageParameters getStorageParameters() {
		return storageParams;
	}
	
	public void update(DirectoryInPlace update, SSStorageParameters storageParams) {
		this.storageParams = storageParams;
		update(update);
	}
}
