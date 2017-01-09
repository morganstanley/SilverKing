package com.ms.silverking.cloud.skfs.dir;

public class DirEntry {
	private final String	name;
	
	public DirEntry(String name) {
		this.name = name;
	}
	
	public RawDirEntry toRawEntry() {
		return RawDirEntry.create((name +"\0").getBytes());
	}
	
	@Override
	public String toString() {
		return name;
	}
}
