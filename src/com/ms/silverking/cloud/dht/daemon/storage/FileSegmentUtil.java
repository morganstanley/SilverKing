package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.client.impl.MetaDataTextUtil;
import com.ms.silverking.cloud.dht.client.serialization.internal.StringMD5KeyCreator;
import com.ms.silverking.cloud.dht.collection.DHTKeyIntEntry;
import com.ms.silverking.cloud.dht.common.CCSSUtil;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.common.RawRetrievalResult;
import com.ms.silverking.io.util.BufferUtil;

public class FileSegmentUtil {
	public FileSegmentUtil() {
	}
	
	public void debugSegment(DHTKey key, File nsDir, int segmentNumber, NamespaceOptions nsOptions) throws IOException {
		FileSegment	segment;
		
		System.out.printf("Segment %d\n", segmentNumber);
		segment = FileSegment.openReadOnly(nsDir, segmentNumber, nsOptions.getSegmentSize(), nsOptions);
		
        for (DHTKeyIntEntry entry : segment.getPKC()) {
        	//System.out.printf("%s\t%s\t%s\n", KeyUtil.keyToString(entry.getKey()), KeyUtil.keyToString(key), entry.getKey().equals(key));
        	if (key == null || entry.getKey().equals(key)) {
	            int     offset;
	            
	            offset = entry.getValue();
	            if (key == null) {
		            if (offset < 0) {
		                OffsetList  offsetList;
		                
		                offsetList = segment.offsetListStore.getOffsetList(-offset);
		                for (int listOffset : offsetList) {
				        	System.out.printf("%s\t%d\n", entry.getKey(), listOffset);
		                    //displayValue(segment.retrieveForDebug(key, listOffset));
		                }
		            } else {
			        	System.out.printf("%s\t%d\n", entry.getKey(), offset);
	                    //displayValue(segment.retrieveForDebug(key, offset));
		            }
	            } else {
		        	System.out.printf("%s\t%d\n", KeyUtil.keyToString(key), offset);
		            if (offset < 0) {
		                OffsetList  offsetList;
		                
		                offsetList = segment.offsetListStore.getOffsetList(-offset);
		                for (int listOffset : offsetList) {
		                    displayValue(segment.retrieveForDebug(key, listOffset));
		                }
		            } else {
	                    displayValue(segment.retrieveForDebug(key, offset));
		            }
	            }
        	}
        }	
	}

	private void displayValue(ByteBuffer value) {
		RawRetrievalResult	rawResult;
		ByteBuffer			aValue;
		
		aValue = BufferUtil.convertToArrayBacked(value);
		//System.out.printf("value: %s\n", StringUtil.byteBufferToHexString(value));
		rawResult = new RawRetrievalResult(RetrievalType.VALUE_AND_META_DATA);
		rawResult.setStoredValue_direct(aValue);
		System.out.printf("ss: %x\n", CCSSUtil.getStorageState(rawResult.getCCSS())); 
		System.out.printf("%s\n", MetaDataTextUtil.toMetaDataString(rawResult.getMetaData(), true));
	}
	
	private static NamespaceOptions readNamespaceOptions(File nsDir) {
        try {
        	NamespaceProperties	nsProperties;
        	
            nsProperties = NamespacePropertiesIO.read(nsDir);
    		return nsProperties.getOptions();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
	}

	public static void debugFiles(DHTKey key, File nsDir, int minSegment, int maxSegment) throws IOException {
		NamespaceOptions nsOptions;
		
		nsOptions = readNamespaceOptions(nsDir);
		for (int i = minSegment; i <= maxSegment; i++) {
			try {
				new FileSegmentUtil().debugSegment(key, nsDir, i, nsOptions);
			} catch (FileNotFoundException fnfe) {
				System.out.printf("Ignoring FileNotFoundException for segment %d\n", i);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		if (args.length != 4) {
			System.err.println("args: <key> <nsDir> <minSegment> <maxSegment>");
		} else {
			try {
				DHTKey	key;
				File	nsDir;
				int		minSegment;
				int		maxSegment;
				
				if (!args[0].equals("*")) {
					key = new StringMD5KeyCreator().createKey(args[0]);
				} else {
					key = null;
				}
				nsDir = new File(args[1]);
				minSegment = Integer.parseInt(args[2]);
				maxSegment = Integer.parseInt(args[3]);
				debugFiles(key, nsDir, minSegment, maxSegment);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
