package com.ms.silverking.cloud.skfs.dir;

import java.io.IOException;

import com.google.common.io.LittleEndianDataOutputStream;
import com.ms.silverking.numeric.NumConversion;

/*
typedef struct DirEntry {
	uint16_t	magic;
	uint16_t	size;
	const char	*data;
} DirEntry;

#define DE_MAGIC	0xadab

*/

public class RawDirEntry {
	private final int		magic;
	private final short		size;
	private final byte[]	data;
	
	private static final int	DE_MAGIC = 0xad00ab00;
	
	public RawDirEntry(int magic, short size, byte[] data) {
		this.magic = magic;
		this.size = size;
		this.data = data;
		// not validating magic == DE_MAGIC and size == data.length 
		// since we may use this class to read possibly invalid entries
	}
	
	public RawDirEntry(short size, byte[] data) {
		this(DE_MAGIC, size, data);
	}
	
	public static RawDirEntry create(byte[] data) {
		if (data.length > Short.MAX_VALUE) {
			throw new RuntimeException("data.length > Short.MAX_VALUE");
		} else {
			return new RawDirEntry((short)data.length, data);
		}
	}
	
	public int getSerializedSize() {
		return NumConversion.BYTES_PER_INT + NumConversion.BYTES_PER_SHORT + size;
	}
	
	public void write(LittleEndianDataOutputStream out) throws IOException {
		out.writeInt(magic);
		out.writeShort(size);
		out.write(data);
	}

	public long serialize(byte[] buf, int offset) {
		NumConversion.intToBytesLittleEndian(magic, buf, offset);
		NumConversion.shortToBytesLittleEndian(size, buf, offset + NumConversion.BYTES_PER_INT);
		System.arraycopy(data, 0, buf, offset + NumConversion.BYTES_PER_INT + NumConversion.BYTES_PER_SHORT, data.length);
		return getSerializedSize();
	}
}
