package com.ms.silverking.cloud.skfs.dir.serverside;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.crypto.MD5KeyDigest;
import com.ms.silverking.cloud.dht.daemon.storage.StorageParameters;
import com.ms.silverking.cloud.dht.serverside.PutTrigger;
import com.ms.silverking.cloud.dht.serverside.RetrieveTrigger;
import com.ms.silverking.cloud.dht.serverside.SSNamespaceStore;
import com.ms.silverking.cloud.dht.serverside.SSRetrievalOptions;
import com.ms.silverking.cloud.dht.serverside.SSStorageParameters;
import com.ms.silverking.cloud.dht.serverside.SSUtil;
import com.ms.silverking.cloud.skfs.dir.DirectoryInPlace;
import com.ms.silverking.compression.CompressionUtil;
import com.ms.silverking.log.Log;

public class DirectoryServer implements PutTrigger, RetrieveTrigger {
	private final Map<DHTKey, DirectoryInMemorySS>	directories;
	
	private static final MD5KeyDigest	md5KeyDigest;
	
	private static final byte[]	emptyUserData = new byte[0];
	
	static {
		Log.warning("Initialized DirectoryServer class");
		md5KeyDigest = new MD5KeyDigest();
	}
	
	public DirectoryServer() {
		directories = new HashMap<>();
	}
	
	private String stringToKeyString(String s) {
		return KeyUtil.keyToString(md5KeyDigest.computeKey(s));
	}

	@Override
	public OpResult put(SSNamespaceStore nsStore, DHTKey key, ByteBuffer value, SSStorageParameters storageParams, byte[] userData,
			NamespaceVersionMode nsVersionMode) {
		byte[]				buf;
		int					bufOffset;
		int					bufLimit;
		
		Log.warningf("DirectoryServer.put() %s %s %s  %s %s", KeyUtil.keyToString(key), stringToKeyString("/"), stringToKeyString("/skfs"), value.hasArray(), storageParams.getCompression());
		//Log.warningf("compressedSize %d uncompressedSize %d", storageParams.getCompressedSize(), storageParams.getUncompressedSize());

		//System.out.printf("p checksumType %s checksum.length %d\n", storageParams.getChecksumType(), storageParams.getChecksum().length);
		
		//System.out.println();
		//System.out.printf("%s\n", StringUtil.byteBufferToHexString(value));
		buf = value.array();
		bufOffset = value.arrayOffset();
		bufLimit = bufOffset + value.limit();
		//System.out.printf("%s\n", value);
		//System.out.printf("%s\n", StringUtil.byteArrayToHexString(buf, bufOffset, bufLimit));
		//System.out.println();
		
		DirectoryInMemorySS	existingDir;
		DirectoryInPlace	updateDir;
		
		if (storageParams.getCompression() == Compression.NONE) {
			//System.out.printf("No compression\n");
			/*
			buf = value.array();
			bufOffset = value.arrayOffset();
			bufLimit = bufOffset + value.limit();
			System.out.printf("%s\n", value);
			System.out.printf("%s\n", StringUtil.byteArrayToHexString(buf, bufOffset, bufLimit));
			*/
		} else {
			try {
				int	dataOffset;
				
				//System.out.printf("Compression\n");
	            //dataOffset = MetaDataUtil.getDataOffset(value.array(), value.arrayOffset());
				dataOffset = value.position();
				//System.out.printf("%s\n", storageParams);
				//System.out.printf("%s\n", value.toString());
				//System.out.printf("%s\n", StringUtil.byteBufferToHexString(value));
				buf = CompressionUtil.decompress(storageParams.getCompression(), value.array(), dataOffset, storageParams.getCompressedSize(), storageParams.getUncompressedSize());
				//System.out.printf("%s\n", StringUtil.byteArrayToHexString(buf));
				bufOffset = 0;
				bufLimit = buf.length;
			} catch (IOException ioe) {
				Log.logErrorWarning(ioe);
				return OpResult.ERROR;
			}
		}
		updateDir = new DirectoryInPlace(buf, bufOffset, bufLimit);
		existingDir = directories.get(key);
		if (existingDir == null) {
			DirectoryInMemorySS	newDir;
			
			newDir = new DirectoryInMemorySS(updateDir, storageParams);
			directories.put(key, newDir);
		} else {
			existingDir.update(updateDir, storageParams);
		}
		return OpResult.SUCCEEDED;
	}
	

	@Override
	public OpResult putUpdate(SSNamespaceStore nsStore, DHTKey key, long version, byte storageState) {
		//return nsStore.putUpdate(key, version, storageState);
		// FIXME - this needs to actually commit the update...
		return OpResult.SUCCEEDED;
	}
	
	@Override
	public ByteBuffer retrieve(SSNamespaceStore nsStore, DHTKey key, SSRetrievalOptions options) {
		ByteBuffer	rVal;
		
		//return nsStore.retrieve(key, options);
		DirectoryInMemorySS	existingDir;
		
		System.out.printf("retrieve %s\n", KeyUtil.keyToString(key));
		
		existingDir = directories.get(key);
		if (existingDir != null) {
			byte[]	serializedDir;
			StorageParameters	storageParams;
			
			serializedDir = existingDir.serialize();
			//Log.warningf("DirectoryServer.retrieve()"+ buf.hasArray());
			//System.out.printf("%s\n", StringUtil.byteArrayToHexString(serializedDir));
			
			storageParams = StorageParameters.fromSSStorageParameters(existingDir.getStorageParameters(), serializedDir.length, serializedDir.length, Compression.NONE);
			
			rVal = SSUtil.retrievalResultBufferFromValue(serializedDir, storageParams);

			//System.out.printf("r checksumType %s checksum.length %d\n", storageParams.getChecksumType(), storageParams.getChecksum().length);
			//System.out.printf("uc %d\nc  %d\nl  %d\n%s\n", storageParams.getUncompressedSize(), storageParams.getCompressedSize(), serializedDir.length, storageParams.getCompression());
			//System.out.printf("### %s\n", rVal);
			//System.out.printf("rVal %s\n", StringUtil.byteBufferToHexString(rVal));
			return rVal;
		} else {
			return null;
		}
	}
}
