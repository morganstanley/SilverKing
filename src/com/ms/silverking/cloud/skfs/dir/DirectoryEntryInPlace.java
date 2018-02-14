package com.ms.silverking.cloud.skfs.dir;

import java.nio.ByteBuffer;

import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.text.StringUtil;
import com.ms.silverking.util.ArrayUtil;

public class DirectoryEntryInPlace extends DirectoryEntryBase {
	private final byte[]	buf;
	private final int		offset;
	private final short		nameLength;
	
	public DirectoryEntryInPlace(byte[] buf, int offset) {
		this.buf = buf;
		this.offset = offset;
		nameLength = NumConversion.bytesToShortLittleEndian(buf, offset + sizeOffset);
		if (nameLength < 0) {
			System.out.printf("%d=%d+%d\n", offset + sizeOffset, offset, sizeOffset);
			System.out.printf("%s\n", StringUtil.byteArrayToHexString(buf));
			System.out.printf("%s\n", StringUtil.byteArrayToHexString(buf, offset + sizeOffset, NumConversion.BYTES_PER_SHORT));
		}
	}
	
	public int getLengthBytes() {
		return dataOffset + getNameLength();
	}
	
	public DirectoryEntryInPlace duplicate() {
		return new DirectoryEntryInPlace(duplicateBuffer(), 0);
	}
	
	public byte[] duplicateBuffer() {
		byte[]	newBuf;
		
		newBuf = new byte[getLengthBytes()];
		System.arraycopy(buf, offset, newBuf, 0, newBuf.length);
		return newBuf;
	}

	@Override
	public short getMagic() {
		return NumConversion.bytesToShortLittleEndian(buf, offset + magicOffset);
	}
	
	public short getDataSize() {
		return NumConversion.bytesToShortLittleEndian(buf, offset + sizeOffset);
	}
	
    private int getNameOffset() {
    	return offset + dataOffset;
    }

    private int getStatusOffset() {
    	return offset + statusOffset;
    }
    
    private int getVersionOffset() {
    	return offset + versionOffset;
    }    
    
	@Override
	public short getNameLength() {
		return nameLength;
	}

	@Override
	public short getStatus() {
		return NumConversion.bytesToShortLittleEndian(buf, getStatusOffset());
	}
	
	public void setStatus(short status) {
		NumConversion.shortToBytesLittleEndian(status, buf, getStatusOffset());
	}

	@Override
	public long getVersion() {
		return NumConversion.bytesToLongLittleEndian(buf, getVersionOffset());
	}
	
	public void setVersion(long version) {
		NumConversion.longToBytesLittleEndian(version, buf, getVersionOffset());
	}

	@Override
	public String getName() {
		return new String(getNameAsBytes());
	}

	@Override
	public byte[] getNameAsBytes() {
		byte[]	b;
		
		b = new byte[getNameLength()];
		System.arraycopy(buf, getNameOffset(), b, 0, b.length);
		return b;
	}

	@Override
	public ByteBuffer getNameAsByteBuffer() {
		return ByteBuffer.wrap(getNameAsBytes());
	}
	
	public void update(DirectoryEntryInPlace update) {
		long	updateVersion;
		
		updateVersion = update.getVersion();
		if (updateVersion > getVersion()) {
			setVersion(updateVersion);
			setStatus(update.getStatus());
		} else {
			// Stale update; ignore
		}
	}

	public int serialize(byte[] destBuf, int destOffset) {
		int	length;
		
		length = getLengthBytes();
		//System.out.printf("%d\n", length);
		//System.out.printf("%d %d\n", offset, buf.length);
		//System.out.printf("%d %d\n", destOffset, destBuf.length);
		System.arraycopy(buf, offset, destBuf, destOffset, length);
		return length;
	}
	
	@Override
	public int hashCode() {
		return ArrayUtil.hashCode(buf, getNameOffset(), getNameLength());
	}
	
	@Override
	public String toString() {
		return String.format("%s %d %d", getName(), getStatus(), getVersion());
	}
}
