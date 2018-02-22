package com.ms.silverking.cloud.skfs.dir;

import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.log.Log;
import com.ms.silverking.numeric.NumConversion;

public class DirectoryInPlace extends DirectoryBase {
	private final byte[]	buf;
	private final int		offset;
	private final int		limit;
	
	public DirectoryInPlace(byte[] buf, int offset, int limit) {
		this.buf = buf;
		this.offset = offset;
		this.limit = limit;
	}
	
	public Triple<byte[],Integer,Integer> getBufferOffsetLimit() {
		return new Triple<>(buf, offset, limit);
	}

	@Override
	public int getMagic() {
		return NumConversion.bytesToIntLittleEndian(buf, offset + magicOffset);
	}

	@Override
	public int getLengthBytes() {
		return NumConversion.bytesToIntLittleEndian(buf, offset + lengthOffset);
	}

	@Override
	public int getIndexOffset() {
		int	indexOffset;
		
		indexOffset = NumConversion.bytesToIntLittleEndian(buf, offset + indexOffsetOffset);
		return dataOffset + indexOffset;
	}

	@Override
	public int getNumEntries() {
		return NumConversion.bytesToIntLittleEndian(buf, offset + numEntriesOffset);
	}
	
	private int getEntryOffset(int index) {
		//System.out.printf("getEntryOffset(%d)\n", index);
		//System.out.printf("%d %d %d %d\n", getIndexOffset() + indexFirstEntryOffset + index * DEI_ENTRY_SIZE, getIndexOffset(), indexFirstEntryOffset, index * DEI_ENTRY_SIZE);
		return NumConversion.bytesToIntLittleEndian(buf, offset + getIndexOffset() + indexFirstEntryOffset + index * DEI_ENTRY_SIZE);
	}

	@Override
	public DirectoryEntry getEntry(int index) {
		int	offset;
		
		offset = getEntryOffset(index);
		if (offset >= 0) {
			return getEntryAtOffset(offset).getV1();
		} else {
			Log.warning("Bad entry offset in DirectoryInPlace.getEntry()");
			return null;
		}
	}

	@Override
	public Pair<DirectoryEntry, Long> getEntryAtOffset(int offset) {
		if (offset + DirectoryEntryBase.headerSize >= getIndexOffset()) {
			return null;
		} else {
			DirectoryEntryInPlace	entry;
			short	eSize;
			int		nextEntry;
			
			entry = new DirectoryEntryInPlace(buf, this.offset + dataOffset + offset);
			eSize = entry.getNameLength();
			nextEntry = offset + DirectoryEntryBase.headerSize + eSize;
			//System.out.printf("eSize %x %d\n", eSize, eSize);
			//System.out.printf("nextEntry %d\n", nextEntry);
			return new Pair<>(entry, (long)nextEntry);
		}
	}
}
