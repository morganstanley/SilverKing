package com.ms.silverking.cloud.skfs.dir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.ms.silverking.io.StreamParser;

public class SimpleDir {
	List<DirEntry>	entries;
	
	public SimpleDir() {
		entries = new ArrayList<>();
	}
	
	public void add(DirEntry entry) {
		entries.add(entry);
	}
	
	public RawSimpleDir toRawSimpleDir() {
		long				totalLength;
		List<RawDirEntry>	rawEntries;
		
		rawEntries = new ArrayList<>();
		totalLength = 0;
		for (DirEntry entry : entries) {
			RawDirEntry	rawEntry;
			
			rawEntry = entry.toRawEntry();
			rawEntries.add(rawEntry);
			totalLength += rawEntry.getSerializedSize();
		}
		return toRawSimpleDir_(rawEntries, totalLength);
	}
	
	private RawSimpleDir toRawSimpleDir_(List<RawDirEntry> rawEntries, long totalLength) {
		long				curOffset;
		byte[]				data;
		
		if (totalLength > Integer.MAX_VALUE) {
			throw new RuntimeException("Directory lengths > Integer.MAX_VALUE currently not supported");
		}
		data = new byte[(int)totalLength];
		curOffset = 0;
		for (RawDirEntry rawEntry : rawEntries) {
			curOffset += rawEntry.serialize(data, (int)curOffset);
		}
		return new RawSimpleDir(data);
	}
	
	public static SimpleDir readFromFile(File inputFile) throws IOException {
		List<String>	lines;
		SimpleDir		simpleDir;
		
		lines = StreamParser.parseFileLines(inputFile);
		simpleDir = new SimpleDir();
		for (String line : lines) {
			simpleDir.add(new DirEntry(line.trim()));
		}
		return simpleDir;
	}
	
	public static SimpleDir createFrom(File[] files) {
		SimpleDir		simpleDir;
		
		simpleDir = new SimpleDir();
		for (File file : files) {
			simpleDir.add(new DirEntry(file.getName()));
		}
		return simpleDir;		
	}
	
	public static SimpleDir createFrom(String[] fileNames) {
		SimpleDir		simpleDir;
		
		simpleDir = new SimpleDir();
		for (String fileName : fileNames) {
			simpleDir.add(new DirEntry(fileName));
		}
		return simpleDir;		
	}
}
