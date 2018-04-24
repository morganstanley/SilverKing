package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.io.IOException;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.StorageType;
import com.ms.silverking.cloud.dht.collection.DHTKeyIntEntry;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.common.SegmentIndexLocation;
import com.ms.silverking.cloud.dht.daemon.storage.FileSegment.SegmentPrereadMode;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.log.Log;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

/**
 * For testing. Iterate through a file segment's pkc.
 */
public class FileSegmentRecoverer {
    private final File  nsDir;
    
    private static final boolean    verbose = false;
    
    public FileSegmentRecoverer(File nsDir) {
        this.nsDir = nsDir;
    }
    
    FileSegment recoverFullSegment(int segmentNumber, NamespaceStore nsStore, 
    							   SegmentIndexLocation segmentIndexLocation, SegmentPrereadMode segmentPrereadMode) {
    	FileSegment	_segment;
    	
        Log.warningf("Recovering full segment: %d %s", segmentNumber, segmentPrereadMode);
        _segment = null;
        try {
            FileSegment segment;
            Stopwatch   sw;
            
            sw = new SimpleStopwatch();
            segment = FileSegment.openReadOnly(nsDir, segmentNumber, nsStore.getNamespaceOptions().getSegmentSize(), 
                                               nsStore.getNamespaceOptions(), segmentIndexLocation, segmentPrereadMode);            
            for (DHTKeyIntEntry entry : segment.getPKC()) {
                int     offset;
                long    creationTime;
                
                offset = entry.getValue();
                if (offset < 0) {
                    OffsetList  offsetList;
                    
                    offsetList = segment.offsetListStore.getOffsetList(-offset);
                    for (Triple<Integer,Long,Long> offsetVersionAndStorageTime : offsetList.offsetVersionAndStorageTimeIterable()) {
                    	creationTime = offsetVersionAndStorageTime.getV3();
                        nsStore.putSegmentNumberAndVersion(entry.getKey(), segmentNumber, 
                        		offsetVersionAndStorageTime.getV2(), creationTime);
                    }
                } else {
                	long	version;
                	
                    if (nsStore.getNamespaceOptions().getVersionMode() == NamespaceVersionMode.SINGLE_VERSION) {
                    	version = DHTConstants.unspecifiedVersion;
                    } else {
                    	version = segment.getVersion(offset);
                    }
                    if (nsStore.getNamespaceOptions().getRevisionMode() == RevisionMode.UNRESTRICTED_REVISIONS) {
                        creationTime = segment.getCreationTime(offset);
                    } else {
                        creationTime = 0;
                    }
                    nsStore.putSegmentNumberAndVersion(entry.getKey(), segmentNumber, version, creationTime);
                }
            }
            
            //{
                //DataSegmentWalker   dsWalker;
                
                //dsWalker = new DataSegmentWalker(segment.dataBuf);
                //for (DataSegmentWalkEntry entry : dsWalker) {
                    //nsStore.addToSizeStats(entry.getUncompressedLength(), entry.getCompressedLength());
                //}
            //}
            
            if (segmentPrereadMode != SegmentPrereadMode.Preread) {
            	segment.close();
            } else {
            	_segment = segment;
            }
            sw.stop();            
            Log.warning("Done recovering full segment: ", segmentNumber +"\t"+ sw.getElapsedSeconds());
        } catch (IOException ioe) {
            Log.logErrorWarning(ioe, "Unable to recover: "+ segmentNumber);
        }
        return _segment;
    }
    
    FileSegment recoverPartialSegment(int segmentNumber, NamespaceStore nsStore) {
        try {
            DataSegmentWalker       dsWalker;
            FileSegment             fileSegment;
            DataSegmentWalkEntry    lastEntry;
            FileSegment.SyncMode    syncMode;
            
            Log.warning("Recovering partial segment: ", segmentNumber);
            syncMode = nsStore.getNamespaceOptions().getStorageType() == StorageType.FILE_SYNC ? 
                                                    FileSegment.SyncMode.Sync : FileSegment.SyncMode.NoSync;
            fileSegment = FileSegment.openForRecovery(nsDir, segmentNumber, 
                                                      nsStore.getNamespaceOptions().getSegmentSize(), syncMode, 
                                                      nsStore.getNamespaceOptions());
            fileSegment.addReference();
            dsWalker = new DataSegmentWalker(fileSegment.dataBuf);
            lastEntry = null;
            for (DataSegmentWalkEntry entry : dsWalker) {
                if (verbose) {
                    System.out.println(entry);
                }
                fileSegment._put(entry.getKey(), entry.getOffset(), entry.getVersion(), entry.getCreator().getBytes(), 
                				nsStore.getNamespaceOptions());
                //fileSegment.getPKC().put(entry.getKey(), entry.getOffset());
                if (verbose) {
                    System.out.println("setting: "+ entry.getOffset());
                    System.out.println("sanity check: "+ fileSegment.getPKC().get(entry.getKey()));
                }
                lastEntry = entry;
                nsStore.putSegmentNumberAndVersion(entry.getKey(), 
                        segmentNumber, entry.getVersion(), entry.getCreationTime());
                nsStore.addToSizeStats(entry.getUncompressedLength(), entry.getCompressedLength());
            }
            if (lastEntry != null) {
                if (verbose) {
                    System.out.println(lastEntry);
                    System.out.println("setting nextFree: "+ lastEntry.nextEntryOffset());
                }
                fileSegment.setNextFree(lastEntry.nextEntryOffset());
            } else {
                fileSegment.setNextFree(SegmentFormat.headerSize);
            }
            Log.warning("Done recovering partial segment: ", segmentNumber);
            return fileSegment;
        } catch (IOException ioe) {
            Log.logErrorWarning(ioe, "Unable to recover partial: "+ segmentNumber);
            Log.logErrorWarning(ioe);
            return null;
        }
    }
    
    /**
     * Read a segment. Utility method only.
     * @param segmentNumber
     * @param displayEntries
     * @param nsStore
     * @return
     */
    FileSegment readPartialSegment(int segmentNumber, boolean displayEntries) {
        try {
            DataSegmentWalker       dsWalker;
            FileSegment             fileSegment;
            DataSegmentWalkEntry    lastEntry;
            FileSegment.SyncMode    syncMode;
            NamespaceProperties nsProperties;
            NamespaceOptions    nsOptions;
            //DataSegmentWalker       dsWalker;
            
            nsProperties = NamespacePropertiesIO.read(nsDir);
            nsOptions = nsProperties.getOptions();            
            Log.warning("Reading partial segment: ", segmentNumber);
            syncMode = nsOptions.getStorageType() == StorageType.FILE_SYNC ? 
                                                    FileSegment.SyncMode.Sync : FileSegment.SyncMode.NoSync;
            fileSegment = FileSegment.openForRecovery(nsDir, segmentNumber, 
					            						nsOptions.getSegmentSize(), syncMode, 
					            						nsOptions);
            fileSegment.addReference();
            dsWalker = new DataSegmentWalker(fileSegment.dataBuf);
            lastEntry = null;
            for (DataSegmentWalkEntry entry : dsWalker) {
                if (displayEntries || verbose) {
                    System.out.println(entry);
                }
                fileSegment._put(entry.getKey(), entry.getOffset(), entry.getVersion(), entry.getCreator().getBytes(), 
                				nsOptions);
                //fileSegment.getPKC().put(entry.getKey(), entry.getOffset());
                if (verbose) {
                    System.out.println("setting: "+ entry.getOffset());
                    System.out.println("sanity check: "+ fileSegment.getPKC().get(entry.getKey()));
                }
                lastEntry = entry;
            }
            if (lastEntry != null) {
                if (displayEntries || verbose) {
                    System.out.println(lastEntry);
                    System.out.println("setting nextFree: "+ lastEntry.nextEntryOffset());
                }
                fileSegment.setNextFree(lastEntry.nextEntryOffset());
            } else {
                fileSegment.setNextFree(SegmentFormat.headerSize);
            }
            Log.warning("Done reading partial segment: ", segmentNumber);
            return fileSegment;
        } catch (IOException ioe) {
            Log.logErrorWarning(ioe, "Unable to read partial: "+ segmentNumber);
            Log.logErrorWarning(ioe);
            return null;
        }
    }
    
    void readFullSegment(int segmentNumber, SegmentIndexLocation segmentIndexLocation, SegmentPrereadMode segmentPrereadMode) {
    	Stopwatch	sw;
    	
        Log.warning("Reading full segment: ", segmentNumber);
        sw = new SimpleStopwatch();
        try {
            FileSegment segment;
            NamespaceProperties nsProperties;
            NamespaceOptions    nsOptions;
            //DataSegmentWalker       dsWalker;
            
            nsProperties = NamespacePropertiesIO.read(nsDir);
            nsOptions = nsProperties.getOptions();
            segment = FileSegment.openReadOnly(nsDir, segmentNumber, nsOptions.getSegmentSize(), nsOptions, 
            								segmentIndexLocation, segmentPrereadMode);
            for (DHTKeyIntEntry entry : segment.getPKC()) {
                int     offset;
                
                offset = entry.getValue();
                if (offset < 0) {
                    OffsetList  offsetList;
                    
                    offsetList = segment.offsetListStore.getOffsetList(-offset);
                    for (int listOffset : offsetList) {
                        System.out.printf("%s\t%d\t%d\t%f\t*%d\t%d\n", entry, segment.getCreationTime(offset), segment.getVersion(listOffset), KeyUtil.keyEntropy(entry), offset, offsetList);
                        //nsStore.putSegmentNumberAndVersion(entry.getKey(), segmentNumber, segment.getVersion(listOffset));
                    }
                } else {
                    System.out.printf("%s\t%d\t%d\t%f\t%d\n", entry, segment.getCreationTime(offset), segment.getVersion(offset), KeyUtil.keyEntropy(entry), offset);
                    //nsStore.putSegmentNumberAndVersion(entry.getKey(), segmentNumber, segment.getVersion(offset));
                }
            }
            
            //dsWalker = new DataSegmentWalker(segment.dataBuf);
            //for (DataSegmentWalkEntry entry : dsWalker) {
            //    nsStore.addToSizeStats(entry.getUncompressedLength(), entry.getCompressedLength());
            //}
            
            segment.close();
            sw.stop();
            Log.warning("Done reading full segment: ", segmentNumber +" "+ sw);
        } catch (IOException ioe) {
            Log.logErrorWarning(ioe, "Unable to recover: "+ segmentNumber);
        }
    }
    
    public static void main(String[] args) {
        try {
            if (args.length < 2 || args.length > 3) {
                System.out.println("args: <nsDir> <segmentNumber> [maxSegmentNumber]");
                return;
            } else {
                FileSegmentRecoverer   segmentReader;
                File            nsDir;
                int             minSegmentNumber;
                int             maxSegmentNumber;
                
                nsDir = new File(args[0]);
                minSegmentNumber = Integer.parseInt(args[1]);
                if (args.length == 3) {
                    maxSegmentNumber = Integer.parseInt(args[2]);
                } else {
                	maxSegmentNumber = minSegmentNumber;
                }
                segmentReader = new FileSegmentRecoverer(nsDir);
                FileSegment	segment;
                
                for (int segmentNumber = minSegmentNumber; segmentNumber <= maxSegmentNumber; segmentNumber++) {
	                try {
	                	segmentReader.readFullSegment(segmentNumber, SegmentIndexLocation.RAM, SegmentPrereadMode.Preread);
	                } catch (Exception e) {
	                	e.printStackTrace();
	                	System.out.printf("Exception reading full segment. Reading partial.\n");
	                    segment = segmentReader.readPartialSegment(segmentNumber, true);
	                    //segment.persist();
	                }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
