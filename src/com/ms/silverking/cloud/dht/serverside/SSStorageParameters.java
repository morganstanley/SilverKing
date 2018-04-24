package com.ms.silverking.cloud.dht.serverside;

import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;

public interface SSStorageParameters {
	public long getVersion();
	public int getUncompressedSize();
	public int getCompressedSize();
    public Compression getCompression();
    public byte getStorageState();
	public byte[] getChecksum();
	public byte[] getValueCreator();
	public long getCreationTime();
	public ChecksumType getChecksumType();
}
