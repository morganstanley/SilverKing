package com.ms.silverking.cloud.dht.daemon.storage.util;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.client.impl.KeyCreator;
import com.ms.silverking.cloud.dht.client.serialization.internal.StringMD5KeyCreator;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.daemon.storage.DataSegmentWalkEntry;
import com.ms.silverking.cloud.dht.daemon.storage.DataSegmentWalker;
import com.ms.silverking.cloud.dht.daemon.storage.FileSegment;
import com.ms.silverking.cloud.dht.daemon.storage.NamespacePropertiesIO;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.log.Log;

public class KeySearcher {
	private NamespaceOptions	nsOptions;
	private Set<DHTKey>	dhtKeys;
	private PrintStream	out;
	
	private static KeyCreator<String>	keyCreator;

	static {
		keyCreator = new StringMD5KeyCreator();
	}
	
	public KeySearcher(Set<String> keys, NamespaceOptions nsOptions) {
		dhtKeys = keysToDHTKeys(keys);
		out = System.out;
		this.nsOptions = nsOptions;
	}
	
	private static Set<DHTKey> keysToDHTKeys(Set<String> keys) {
		ImmutableSet.Builder<DHTKey>	dhtKeys;
		
		dhtKeys = ImmutableSet.builder();
		for (String key : keys) {
			dhtKeys.add(keyCreator.createKey(key));
		}
		return dhtKeys.build();
	}
	
	public void searchSegments(File path) throws IOException {
		String[]	segmentFileNames;
		
		segmentFileNames = path.list();
		for (String segmentFileName : segmentFileNames) {
			try {
				int	segmentNumber;
				
				segmentNumber = Integer.parseInt(segmentFileName);
				searchSegment(path, segmentNumber);
			} catch (NumberFormatException nfe) {
				Log.fine("Ignoring non-integer segment file: ", segmentFileName);
			}
		}
	}
	
	public void searchSegment(File nsDir, int segmentNumber) throws IOException {
		ByteBuffer	dataBuf;
		
		out.printf("Searching %s %d\n", nsDir, segmentNumber);
        dataBuf = FileSegment.getDataSegment(nsDir, segmentNumber, nsOptions.getSegmentSize());
        searchSegment(new DataSegmentWalker(dataBuf));
	}
	
	public void searchSegment(DataSegmentWalker dsw) {
		for (DataSegmentWalkEntry entry : dsw) {
			if (dhtKeys.contains(entry.getKey())) {
				display(entry);
			}
		}
	}
	
	private void display(DataSegmentWalkEntry entry) {
		out.printf("\t%s\n", entry);
	}

	public static void main(String[] args) {
		try {
			if (args.length != 2) {
				System.err.println("args: <path> <key1,key2...>");
			} else {
				File		path;
				Set<String>	keys;
				NamespaceProperties	nsProperties;
				NamespaceOptions	nsOptions;
				KeySearcher	keySearcher;
				
				path = new File(args[0]);
				keys = CollectionUtil.parseSet(args[1], ",");
		        nsProperties = NamespacePropertiesIO.read(path);
		        nsOptions = nsProperties.getOptions();
		        keySearcher = new KeySearcher(keys, nsOptions);
		        keySearcher.searchSegments(path);
			}		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
