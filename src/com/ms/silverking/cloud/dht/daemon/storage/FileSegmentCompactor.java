package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.TimeAndVersionRetentionPolicy;
import com.ms.silverking.cloud.dht.ValueRetentionPolicy;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.SegmentIndexLocation;
import com.ms.silverking.cloud.dht.daemon.storage.FileSegment.SegmentPrereadMode;
import com.ms.silverking.collection.HashedSetMap;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.log.Log;

public class FileSegmentCompactor {
	private static final boolean	verbose = false;
	
	private static final String	compactionDirName = "compact";
	private static final String	trashDirName = "trash";
	
	/*
	 * Current strategy is to leave the segment size constant, and to count on sparse file support for
	 * actual disk savings. That is, upon compaction, the data will be compacted at the start of the
	 * data segment, and the index will remain at the end of the data segment. In between, sparse
	 * file support should allow the file system to omit allocation of bytes.
	 */
	
	private static File getDir(File nsDir, String subDirName) throws IOException {
		File	subDir;
		
		subDir = new File(nsDir, subDirName);
        if (!subDir.exists()) {
        	if (!subDir.mkdir()) {
                if (!subDir.exists()) {
                	throw new IOException("mkdir failed: "+ nsDir +" "+ subDir);
                }
        	}
        }
        return subDir;
	}
	
	private static File getCompactionDir(File nsDir) throws IOException {
		return getDir(nsDir, compactionDirName);
	}
	
	private static File getTrashDir(File nsDir) throws IOException {
		return getDir(nsDir, trashDirName);
	}
	
	private static File getCompactionFile(File nsDir, int segmentNumber) throws IOException {
		File	compactionDir;
		File	compactionFile;
		
        compactionDir = getCompactionDir(nsDir);
        compactionFile = new File(compactionDir, Integer.toString(segmentNumber));
        return compactionFile;
	}
	
	private static File getTrashFile(File nsDir, int segmentNumber) throws IOException {
		File	trashDir;
		File	trashFile;
		
        trashDir = getTrashDir(nsDir);
        trashFile = new File(trashDir, Integer.toString(segmentNumber));
        return trashFile;
	}
	
	private static void rename(File src, File target) throws IOException {
    	if (!src.renameTo(target)) {
    		throw new IOException("Rename failed: "+ src +" "+ target);
    	}
	}
	
    static FileSegment createCompactedSegment(File nsDir, int segmentNumber, NamespaceOptions nsOptions, int segmentSize,
    						  				 EntryRetentionCheck retentionCheck, HashedSetMap<DHTKey, Triple<Long, Integer, Long>> removedEntries, boolean includeStorageTime) {
        try {
            DataSegmentWalker       dsWalker;
            FileSegment             sourceSegment;
            FileSegment             destSegment;
            
            Log.warning("Compacting segment: ", segmentNumber);
            sourceSegment = FileSegment.openReadOnly(nsDir, segmentNumber, 
					            						nsOptions.getSegmentSize(),  
					            						nsOptions, SegmentIndexLocation.RAM, SegmentPrereadMode.Preread);
            sourceSegment.addReference();
            try {
	            destSegment = FileSegment.create(getCompactionDir(nsDir), segmentNumber, nsOptions.getSegmentSize(), FileSegment.SyncMode.NoSync, nsOptions);
	            
	            dsWalker = new DataSegmentWalker(sourceSegment.dataBuf);
	            for (DataSegmentWalkEntry entry : dsWalker) {
	            	SegmentStorageResult	storageResult;
	            	
	                if (verbose) {
	                    System.out.println(entry);
	                }
	                if (retentionCheck.shouldRetain(segmentNumber, entry)) {
		                if (verbose) {
		                    System.out.println("setting: "+ entry.getOffset());
		                    System.out.println("sanity check: "+ sourceSegment.getPKC().get(entry.getKey()));
		                	Log.warning("Retaining:\t", entry.getKey());
		                }
		                
		                storageResult = destSegment.putFormattedValue(entry.getKey(), entry.getStoredFormat(), entry.getStorageParameters(), nsOptions);
		                if (storageResult != SegmentStorageResult.stored) {
		                	// FUTURE - think about duplicate stores, and the duplicate store WritableSegmentBase
		                	if (storageResult != SegmentStorageResult.duplicateStore) {
		                		throw new RuntimeException("Compaction failed: "+ storageResult);
		                	} else {
		                		if (Log.levelMet(Level.FINE)) {
		                			Log.finef("Duplicate store in compaction %s", entry.getKey());
		                		}
		                	}
		                }
	                } else {
		                if (verbose) {
		                	Log.warning("Dropping: \t", entry.getKey());
		                }
		                removedEntries.addValue(entry.getKey(), new Triple<>(entry.getVersion(), segmentNumber, includeStorageTime ? entry.getCreationTime() : 0));
	                }
	            }
            } finally {
            	sourceSegment.removeReference();
            }
            Log.warning("Done compacting segment: ", segmentNumber);
            return destSegment;
        } catch (IOException ioe) {
            Log.logErrorWarning(ioe, "Unable to compact: "+ segmentNumber);
            throw new RuntimeException(ioe);
        }
    }
    
    public static HashedSetMap<DHTKey, Triple<Long, Integer, Long>> compact(File nsDir, int segmentNumber, NamespaceOptions nsOptions, 
			  				   EntryRetentionCheck retentionCheck) throws IOException {
    	FileSegment	compactedSegment;
    	File		oldFile;
    	File		trashFile;
    	File		newFile;
    	HashedSetMap<DHTKey, Triple<Long, Integer, Long>>	removedEntries;
    	
    	removedEntries = new HashedSetMap<>();
    	compactedSegment = createCompactedSegment(nsDir, segmentNumber, nsOptions, nsOptions.getSegmentSize(), retentionCheck, 
    											  removedEntries, nsOptions.getRevisionMode() == RevisionMode.UNRESTRICTED_REVISIONS);
    	compactedSegment.persist();
        Log.warning("Swapping to compacted segment: ", segmentNumber);
    	oldFile = FileSegment.fileForSegment(nsDir, segmentNumber);
    	newFile = getCompactionFile(nsDir, segmentNumber);
    	trashFile = getTrashFile(nsDir, segmentNumber);
    	rename(oldFile, trashFile); // Leave old file around for one cycle in case there are references to it
    	rename(newFile, oldFile);
        Log.warning("Done swapping to compacted segment: ", segmentNumber);
        return removedEntries;
    }
    
	public static void delete(File nsDir, int segmentNumber) throws IOException {
    	File		oldFile;
    	File		trashFile;
    	
        Log.warning("Deleting segment: ", segmentNumber);
    	oldFile = FileSegment.fileForSegment(nsDir, segmentNumber);
    	trashFile = getTrashFile(nsDir, segmentNumber);
    	rename(oldFile, trashFile); // Leave old file around for one cycle in case there are references to it
        Log.warning("Done deleting segment: ", segmentNumber);
	}    
    
    public static void emptyTrashAndCompaction(File nsDir) {
    	try {
    		emptyDir(getTrashDir(nsDir));
    		emptyDir(getCompactionDir(nsDir));
    	} catch (IOException ioe) {
    		Log.logErrorWarning(ioe);
    	}
    }
    
    private static void emptyDir(File dir) {
    	File[]	files;
    	
    	files = dir.listFiles();
    	for (File file : files) {
    		if (!file.delete()) {
    			Log.warning("Failed to delete", file);
    		}
    	}
    }
    
    public static void main(String[] args) {
    	try {
    		if (args.length != 3) {
    			System.out.println("args: <nsDir> <segmentNumber> <timeSpanSeconds>");
    		} else {
	    		File				nsDir;
	    		int					segmentNumber;
	    		NamespaceOptions	nsOptions;
	    		ValueRetentionPolicy	valueRetentionPolicy;
	    		int						timeSpanSeconds;
	    		
	    		nsDir = new File(args[0]);
	    		segmentNumber = Integer.parseInt(args[1]);
	    		timeSpanSeconds = Integer.parseInt(args[2]);
	    		valueRetentionPolicy = new TimeAndVersionRetentionPolicy(TimeAndVersionRetentionPolicy.Mode.wallClock, 1, timeSpanSeconds);
	    		nsOptions = DHTConstants.defaultNamespaceOptions.valueRetentionPolicy(valueRetentionPolicy);
	    		compact(nsDir, segmentNumber, nsOptions, new TestRetentionCheck(32768));
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
}
