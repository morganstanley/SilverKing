package com.ms.silverking.cloud.skfs.dir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.google.common.io.LittleEndianDataOutputStream;
import com.ms.silverking.numeric.NumConversion;

/*
typedef struct OpenDir {
	uint16_t	magic;
	uint64_t	length;
	const char	*data;
} OpenDir;

#define OD_MAGIC	0xdaba
#define OD_MAGIC_BYTES	2

*/

public class RawSimpleDir {
	private final short		magic;
	private final long		length;
	private final byte[]	data;
	
	private static final short	OD_MAGIC = (short)0xdaba;
	
	public RawSimpleDir(short magic, long length, byte[] data) {
		this.magic = magic;
		this.length = length;
		this.data = data;
	}
	
	public RawSimpleDir(long length, byte[] data) {
		this(OD_MAGIC, length, data);
	}
	
	public RawSimpleDir(byte[] data) {
		this(data.length, data);
	}
	
	public void write(LittleEndianDataOutputStream out) throws IOException {
		out.writeShort(magic);
		out.writeLong(length);
		out.write(data);
	}
	
	public int getSerializedSize() {
		return data.length + NumConversion.BYTES_PER_LONG + NumConversion.BYTES_PER_SHORT;
	}
	
	public byte[] serialize() {
		ByteArrayOutputStream	out;
		
		out = new ByteArrayOutputStream(getSerializedSize());
		try {
			write(new LittleEndianDataOutputStream(out));
		} catch (IOException ioe) {
			throw new RuntimeException("Unexpected exception", ioe);
		}
		return out.toByteArray();
	}
}
