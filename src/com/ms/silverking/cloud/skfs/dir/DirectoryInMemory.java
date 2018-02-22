package com.ms.silverking.cloud.skfs.dir;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.ms.silverking.collection.Pair;

public class DirectoryInMemory extends DirectoryBase {
	private final SortedMap<String,DirectoryEntryInPlace>	entries;
	private int	entryBytes;
	
	public DirectoryInMemory(DirectoryInPlace d) {
		int	numEntries;
		
		entries = new TreeMap<>();
		if (d != null) {
			numEntries = d.getNumEntries();
			for (int i = 0; i < numEntries; i++) {
				DirectoryEntryInPlace	entry;
				
				entry = (DirectoryEntryInPlace)d.getEntry(i);
				addEntry(entry.getName(), entry);
			}
		}
	}
	
	public DirectoryInMemory(byte[] buf, int offset, int limit) {
		this(new DirectoryInPlace(buf, offset, limit));
	}

	public DirectoryInMemory() {
		entries = new TreeMap<>();
	}
	
	@Override
	public int getMagic() {
		return DEI_MAGIC;
	}

	@Override
	public int getLengthBytes() {
		return entryBytes;
	}

	@Override
	public int getIndexOffset() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getNumEntries() {
		return entries.size();
	}

	@Override
	public DirectoryEntry getEntry(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Pair<DirectoryEntry, Long> getEntryAtOffset(int offset) {
		throw new UnsupportedOperationException();
	}
	
	private void addEntry(String name, DirectoryEntryInPlace entry) {
		entries.put(name, entry.duplicate());
		entryBytes += entry.getLengthBytes(); 
	}
	
	public void update(DirectoryInPlace update) {
		for (int i = 0; i < update.getNumEntries(); i++) {
			update((DirectoryEntryInPlace)update.getEntry(i));
		}
	}
	
	public void update(DirectoryEntryInPlace update) {
		DirectoryEntryInPlace	entry;
		String					name;
		
		name = update.getName();
		entry = entries.get(name);
		if (entry == null) {
			addEntry(name, update);
		} else {
			entry.update(update);
		}
	}
	
	private int computeSerializedSize() {
		return headerSize + entryBytes + computeIndexSizeBytes(entries.size()); 
	}
	
	public byte[] serialize() {
		byte[]	buf;
		int		offset;
		int[]	indexOffsets;
		int		totalBytesWritten;
		int		entryIndex;
		int		indexOffset;
		
		buf = new byte[computeSerializedSize()];
		indexOffsets = new int[entries.size()];

		// Write entries, record offsets
		entryIndex = 0;
		offset = dataOffset;
		for (Map.Entry<String,DirectoryEntryInPlace> entry : entries.entrySet()) {
			//System.out.println(entry.getKey());
			//System.out.println("\t"+ entry.getValue());
			indexOffsets[entryIndex++] = offset - dataOffset;
			offset += entry.getValue().serialize(buf, offset);
		}
		// Record index offset
		indexOffset = offset - dataOffset;
		//System.out.printf("indexOffset %d %x\n", indexOffset, indexOffset);
		totalBytesWritten = offset - dataOffset;
		if (totalBytesWritten != entryBytes) {
			throw new RuntimeException(String.format("SerializationException: totalBytesWritten != entryBytes %d != %d", totalBytesWritten, entryBytes));
		}
		
		// Write index using recorded offsets
		totalBytesWritten += writeIndex(buf, indexOffset, indexOffsets);
		//System.out.printf("totalBytesWritten(1) %d %x\n", totalBytesWritten + headerSize, totalBytesWritten + headerSize);
		
		// Write header
		totalBytesWritten += writeHeader(buf, 0, totalBytesWritten /* does not include header */, indexOffset, entries.size());
		//System.out.printf("totalBytesWritten %d %x\n", totalBytesWritten, totalBytesWritten);
		
		return buf;
	}
}
