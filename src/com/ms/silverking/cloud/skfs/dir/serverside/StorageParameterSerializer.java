package com.ms.silverking.cloud.skfs.dir.serverside;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.common.CCSSUtil;
import com.ms.silverking.cloud.dht.daemon.storage.StorageParameters;
import com.ms.silverking.cloud.dht.serverside.SSStorageParameters;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.numeric.NumConversion;

public class StorageParameterSerializer {	
	private static final int	BASE_SERIALIZED_SIZE = NumConversion.BYTES_PER_LONG * 2 
												+ NumConversion.BYTES_PER_INT * 2 
												+ NumConversion.BYTES_PER_SHORT 
												+ ValueCreator.BYTES;
	
	public static int getSerializedLength(SSStorageParameters p) {
		return BASE_SERIALIZED_SIZE + p.getChecksumType().length();
	}
	
	public static byte[] serialize(SSStorageParameters p) {
		ByteBuffer	buf;
		
		buf = ByteBuffer.allocate(getSerializedLength(p));
		buf.putLong(p.getVersion());
		buf.putInt(p.getUncompressedSize());
		buf.putInt(p.getCompressedSize());
		buf.putLong(p.getCreationTime());
		buf.putShort(CCSSUtil.createCCSS(p.getCompression(), p.getChecksumType(), p.getStorageState()));
		buf.put(p.getValueCreator());
		buf.put(p.getChecksum());
		return buf.array();
	}

	public static SSStorageParameters deserialize(byte[] a) {
		ByteBuffer	b;
		long	version;
		int		uncompressedSize;
		int		compressedSize;
		short	ccss;
		byte[]	checksum;
		byte[]	valueCreator;
		long	creationTime;
		
		b = ByteBuffer.wrap(a);
		version = b.getLong();
		uncompressedSize = b.getInt();
		compressedSize = b.getInt();
		creationTime = b.getLong();
		ccss = b.getShort();
		valueCreator = BufferUtil.arrayCopy(b, ValueCreator.BYTES);
		checksum = BufferUtil.arrayCopy(b, CCSSUtil.getChecksumType(ccss).length());
		return new StorageParameters(version, uncompressedSize, compressedSize, ccss, checksum, valueCreator, creationTime);
	}
	
	public static void main(String[] args) {
		StorageParameters	p1;
		SSStorageParameters	p2;
		byte[]				checksum;
		byte[]				valueCreator;
		byte[]				s;
		
		checksum = new byte[ChecksumType.MD5.length()];
		valueCreator = new byte[ValueCreator.BYTES];
		p1 = new StorageParameters(1, 2, 3, CCSSUtil.createCCSS(Compression.NONE, ChecksumType.MD5, 0), checksum, valueCreator, System.currentTimeMillis());
		System.out.println(p1);
		s = serialize(p1);
		p2 = deserialize(s);
		System.out.println(p2);
	}
}
