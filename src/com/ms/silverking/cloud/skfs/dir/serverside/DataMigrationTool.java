package com.ms.silverking.cloud.skfs.dir.serverside;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceServerSideCode;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.common.CCSSUtil;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.daemon.storage.DataSegmentWalkEntry;
import com.ms.silverking.cloud.dht.daemon.storage.DataSegmentWalker;
import com.ms.silverking.cloud.dht.daemon.storage.FileSegment;
import com.ms.silverking.cloud.dht.daemon.storage.NamespacePropertiesIO;
import com.ms.silverking.cloud.dht.daemon.storage.StorageParameters;
import com.ms.silverking.cloud.dht.serverside.SSStorageParameters;
import com.ms.silverking.cloud.skfs.dir.DirectoryInPlace;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.io.FileUtil;
import com.ms.silverking.io.util.BufferUtil;

public class DataMigrationTool {
	public DataMigrationTool() {
	}
	
	public void walk(File sourceDir, File destDir, File savedSegmentsDir) throws IOException {
		List<Long>	segments;
		
		segments = FileUtil.numericFilesInDirAsSortedLongList(sourceDir);
		for (long segment : segments) {
			walk(sourceDir, destDir, (int)segment);
		}
		
		for (long segment : segments) {
			File	segmentFile;
			File	savedSegmentFile;
			
			segmentFile = new File(sourceDir, Long.toString(segment));
			savedSegmentFile = new File(savedSegmentsDir, Long.toString(segment));
			if (!segmentFile.renameTo(savedSegmentFile)) {
				throw new RuntimeException("Failed to rename "+ segmentFile.getAbsolutePath() +" to "+ savedSegmentFile.getAbsolutePath());
			}
		}
		
		modifyProperties(sourceDir);
	}
	
	private void modifyProperties(File sourceDir) throws IOException {
		NamespaceProperties	originalProperties;
		NamespaceOptions	originalOptions;
		NamespaceProperties	modifiedProperties;
		NamespaceOptions	modifiedOptions;
		
		originalProperties = NamespacePropertiesIO.read(sourceDir);
		originalOptions = originalProperties.getOptions();
		
		modifiedOptions = originalOptions.namespaceServerSideCode(new NamespaceServerSideCode("", "com.ms.silverking.cloud.skfs.dir.serverside.DirectoryServer", "com.ms.silverking.cloud.skfs.dir.serverside.DirectoryServer"));
		modifiedProperties = originalProperties.options(modifiedOptions);
		
		NamespacePropertiesIO.rewrite(sourceDir, modifiedProperties);
	}
	
	public void walk(File nsDir, File ssDir, int segmentNumber) throws IOException {
        ByteBuffer  dataBuf;
        DataSegmentWalker   dsWalker;
        NamespaceProperties nsProperties;
        NamespaceOptions    nsOptions;
        
        nsProperties = NamespacePropertiesIO.read(nsDir);
        nsOptions = nsProperties.getOptions();
        dataBuf = FileSegment.getDataSegment(nsDir, segmentNumber, nsOptions.getSegmentSize());
        dsWalker = new DataSegmentWalker(dataBuf);
        while (dsWalker.hasNext()) {
            DataSegmentWalkEntry  entry;
            
            entry = dsWalker.next();
            System.out.println(entry.getOffset() +" "+ entry);
            migrateEntry(entry, ssDir);
        }
	}
	
	private void migrateEntry(DataSegmentWalkEntry entry, File ssDir) throws IOException {
		ByteBuffer	valueBuf;
		byte[]		value;
		File		dirDir;
		StorageParameters	newSP;
		StorageParameters	o;
		
		//System.out.printf("\n ------------- \n%s\n", StringUtil.byteBufferToHexString(entry.getStoredFormat()));
		valueBuf = entry.getValue();
		value = BufferUtil.arrayCopy(valueBuf);
		//System.out.printf("\n......... \n%s\n", StringUtil.byteArrayToHexString(value));
		File	destFile;
		
		dirDir = new File(ssDir, KeyUtil.keyToString(entry.getKey()));
		if (!dirDir.exists()) {
			if (!dirDir.mkdir()) {
				throw new RuntimeException("Unable to create: "+ dirDir);
			}
		}
		o = entry.getStorageParameters();
		newSP = new StorageParameters(o.getVersion(), value.length, value.length, CCSSUtil.createCCSS(Compression.NONE, o.getChecksumType(), o.getStorageState()), o.getChecksum(), o.getValueCreator(), o.getCreationTime());
		destFile = new File(dirDir, Long.toString(entry.getVersion()));
		FileUtil.writeToFile(destFile, StorageParameterSerializer.serialize(newSP), value);
		
		DirectoryInMemorySS	dim;
		Pair<SSStorageParameters, byte[]>	p;
		DirectoryInPlace	dip;
		
		dim = new DirectoryInMemorySS(entry.getKey(), null, entry.getStorageParameters(), new File(ssDir, KeyUtil.keyToString(entry.getKey())), null, false);
		p = dim.readFromDisk(entry.getStorageParameters().getVersion());
		System.out.println(p.getV1());
		//System.out.println(StringUtil.byteArrayToHexString(p.getV2()));
		dip = new DirectoryInPlace(p.getV2(), 0, p.getV2().length);
		dip.display();
	}

	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("args: <data dir>");
		} else {
			try {
				File	sourceDir;
				File	destDir;
				File	savedSegmentsDir;
				
				sourceDir = new File(args[0]);
				if (!sourceDir.exists()) {
					throw new RuntimeException("sourceDir doesn't exist: "+ sourceDir.getAbsolutePath());
				}
				destDir = new File(sourceDir, DHTConstants.ssSubDirName);
				if (!destDir.exists()) {
					if (!destDir.mkdir()) {
						throw new RuntimeException("Couldn't create destDir: "+ destDir.getAbsolutePath());
					}
				} else {
					if (destDir.list().length > 0) {
						throw new RuntimeException("destDir not empty: "+ destDir.getAbsolutePath());
					}
				}
				savedSegmentsDir = new File(sourceDir, "segments.saved");
				if (!savedSegmentsDir.exists()) {
					if (!savedSegmentsDir.mkdir()) {
						throw new RuntimeException("Couldn't create savedSegmentsDir: "+ savedSegmentsDir.getAbsolutePath());
					}
				} else {
					if (savedSegmentsDir.list().length > 0) {
						throw new RuntimeException("savedSegmentsDir not empty: "+ savedSegmentsDir.getAbsolutePath());
					}
				}
				new DataMigrationTool().walk(sourceDir, destDir, savedSegmentsDir);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
