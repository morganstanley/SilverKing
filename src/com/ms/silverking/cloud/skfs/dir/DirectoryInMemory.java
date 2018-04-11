package com.ms.silverking.cloud.skfs.dir;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import com.ms.silverking.cloud.skfs.dir.serverside.ByteString;
import com.ms.silverking.collection.Pair;

public class DirectoryInMemory extends DirectoryBase {
	private final SortedMap<ByteString,DirectoryEntryInPlace>	entries;
	private int	entryBytes;
	
	private static final double	largeUpdateThreshold = 0.2;
	
	public DirectoryInMemory(DirectoryInPlace d) {
		int	numEntries;
		
		entries = new TreeMap<>();
		if (d != null) {
			numEntries = d.getNumEntries();
			for (int i = 0; i < numEntries; i++) {
				DirectoryEntryInPlace	entry;
				
				entry = (DirectoryEntryInPlace)d.getEntry(i);
				addEntry(entry.getNameAsByteString(), entry);
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
	
	private void addEntry(ByteString name, DirectoryEntryInPlace entry) {
		// copy so that the source can be garbage collected
		entries.put(name.duplicateBuffer(), entry.duplicate());
		entryBytes += entry.getLengthBytes();
	}
	
	public void update(DirectoryInPlace update) {
		//update.display();
		//System.out.printf("%d %d %s\n", update.getNumEntries(), entries.size(), ((double)update.getNumEntries() / (double)entries.size() > largeUpdateThreshold));
		if ((double)update.getNumEntries() / (double)entries.size() > largeUpdateThreshold) {
			largeUpdate(update);
		} else {
			smallUpdate(update);
		}
	}
	
	private void smallUpdate(DirectoryInPlace update) {
		for (int i = 0; i < update.getNumEntries(); i++) {
			update((DirectoryEntryInPlace)update.getEntry(i));
		}
	}
	
	public void update(DirectoryEntryInPlace update) {
		DirectoryEntryInPlace	entry;
		ByteString				name;
		
		name = update.getNameAsByteString();
		entry = entries.get(name);
		if (entry == null) {
			addEntry(name, update);
		} else {
			entry.update(update);
		}
	}
	
	private void largeUpdate(DirectoryInPlace update) {
		int	i1; // index into update
		DirectoryEntryInPlace	e0; // entry in this object
		DirectoryEntry			e1; // entry in update
		Iterator<DirectoryEntryInPlace>	localEntries;
		int	numUpdateEntries;
		List<DirectoryEntryInPlace>	entriesToAdd;
		
		entriesToAdd = null;
		localEntries = entries.values().iterator();
		e0 = localEntries.hasNext() ? localEntries.next() : null;
		i1 = 0;
		numUpdateEntries = update.getNumEntries();
		while (i1 < numUpdateEntries) {
			int	comp;
			
			e1 = update.getEntry(i1);
			comp = compareForUpdate(e0, e1);
			//System.out.printf("##\t%s\t%d\t%s\t%d\n", e0, i1, e1, comp);
			if (comp < 0) {
				e0 = localEntries.hasNext() ? localEntries.next() : null;
			} else if (comp > 0) {
				if (entriesToAdd == null) {
					entriesToAdd = new ArrayList<>();
				}
				entriesToAdd.add((DirectoryEntryInPlace)e1);
				i1++;
			} else {
				e0.update((DirectoryEntryInPlace)e1);
				e0 = localEntries.hasNext() ? localEntries.next() : null;
				i1++;
			}
		}
		if (entriesToAdd != null) {
			for (DirectoryEntryInPlace e : entriesToAdd) {
				addEntry(e.getNameAsByteString(), e);
			}
		}
	}
	
	private int compareForUpdate(DirectoryEntryInPlace e0, DirectoryEntry e1) {
		ByteString	n0;
		ByteString	n1;
		
		n0 = getEntryNameForUpdate(e0);
		n1 = getEntryNameForUpdate(e1);
		if (n1 == null) {
			// We're spinning through update entries, so we should never find a null entry
			throw new RuntimeException("Unexpected null update entry name");
		} else {
			if (n0 == null) {
				return 1; // to force addition/itereation through updates
			} else {
				return n0.compareTo(n1);
			}
		}
	}

	private ByteString getEntryNameForUpdate(DirectoryEntry e) {
		if (e == null) {
			return null;
		} else {
			return e.getNameAsByteString();
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
		for (Map.Entry<ByteString,DirectoryEntryInPlace> entry : entries.entrySet()) {
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
