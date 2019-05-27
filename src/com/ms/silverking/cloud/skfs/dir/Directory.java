package com.ms.silverking.cloud.skfs.dir;

import com.ms.silverking.collection.Pair;

public interface Directory {
	public int getMagic();
	public int getLengthBytes();
	public int getIndexOffset();
	public int getNumEntries();
	public DirectoryEntry getEntry(int index);
	public Pair<DirectoryEntry,Long> getEntryAtOffset(int offset);
}
