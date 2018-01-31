package com.ms.silverking.cloud.dht.serverside;

import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.daemon.storage.StorageParameters;

public interface SSStorageParameters {
	public long getVersion();
	public int getUncompressedSize();
	public boolean compressedSizeSet();
	public int getCompressedSize();
    public Compression getCompression();
    public byte getStorageState();
	public byte[] getChecksum();
	public byte[] getValueCreator();
	public long getCreationTime();
	public ChecksumType getChecksumType();
	public StorageParameters version(long version);
}
