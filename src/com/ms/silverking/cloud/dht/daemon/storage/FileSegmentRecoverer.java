package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.io.IOException;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.StorageType;
import com.ms.silverking.cloud.dht.collection.DHTKeyIntEntry;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.daemon.storage.FileSegment.BufferMode;
import com.ms.silverking.log.Log;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

/**
 * For testing. Iterate through a file segment's pkc.
 */
public class FileSegmentRecoverer {
    private final File  nsDir;
    private BufferMode  bufferMode;
    
    private static final boolean    verbose = true;
    
    private static final double inPlaceLimitSeconds = 2.0;
    private static final double preReadLimitSeconds = 0.5;
    
    public FileSegmentRecoverer(File nsDir) {
        this.nsDir = nsDir;
        bufferMode = BufferMode.PreRead;
    }
    
    /*
    // recovery of partial
    public FileSegment recoverPartial(int segmentNumber) throws IOException {
        FileSegment segment;
        
        //segment = FileSegment.openReadOnly(nsDir, segmentNumber);
        segment = FileSegment.openForRecovery(nsDir, segmentNumber);
        for (DHTKeyIntEntry entry : segment.getPKC()) {
            segment.getPKC().put(entry.getKey(), entry.getValue());
        //    System.out.println(entry);
        }
        return segment;
    }
    */
    
    
    void recoverFullSegment(int segmentNumber, NamespaceStore nsStore) {
        Log.warning("Recovering full segment: ", segmentNumber);
        try {
            FileSegment segment;
            Stopwatch   sw;
            
            sw = new SimpleStopwatch();
            segment = FileSegment.openReadOnly(nsDir, segmentNumber, nsStore.getNamespaceOptions().getSegmentSize(), 
                                               nsStore.getNamespaceOptions(), bufferMode);            
            for (DHTKeyIntEntry entry : segment.getPKC()) {
                int     offset;
                long    creationTime;
                
                offset = entry.getValue();
                if (offset < 0) {
                    OffsetList  offsetList;
                    
                    offsetList = segment.offsetListStore.getOffsetList(-offset);
                    for (int listOffset : offsetList) {
                        if (nsStore.getNamespaceOptions().getRevisionMode() == RevisionMode.UNRESTRICTED_REVISIONS) {
                            creationTime = segment.getCreationTime(listOffset);
                        } else {
                            creationTime = 0;
                        }
                        nsStore.putSegmentNumberAndVersion(entry.getKey(), segmentNumber, 
                                segment.getVersion(listOffset), creationTime);
                    }
                } else {
                    if (nsStore.getNamespaceOptions().getRevisionMode() == RevisionMode.UNRESTRICTED_REVISIONS) {
                        creationTime = segment.getCreationTime(offset);
                    } else {
                        creationTime = 0;
                    }
                    nsStore.putSegmentNumberAndVersion(entry.getKey(), segmentNumber, 
                            segment.getVersion(offset), creationTime);
                }
            }
            
            //{
                //DataSegmentWalker   dsWalker;
                
                //dsWalker = new DataSegmentWalker(segment.dataBuf);
                //for (DataSegmentWalkEntry entry : dsWalker) {
                    //nsStore.addToSizeStats(entry.getUncompressedLength(), entry.getCompressedLength());
                //}
            //}
            
            segment.close();
            sw.stop();
            Log.warning("Done recovering full segment: ", segmentNumber +"\t"+ sw.getElapsedSeconds());
            if (bufferMode == BufferMode.InPlace) {
                if (sw.getElapsedSeconds() > inPlaceLimitSeconds) {
                    bufferMode = BufferMode.PreRead;
                }
            } else {
                if (sw.getElapsedSeconds() < preReadLimitSeconds) {
                    bufferMode = BufferMode.InPlace;
                }
            }
        } catch (IOException ioe) {
            Log.logErrorWarning(ioe, "Unable to recover: "+ segmentNumber);
        }
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
     * @param nsStore
     * @return
     */
    FileSegment readPartialSegment(int segmentNumber) {
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
            Log.warning("Recovering partial segment: ", segmentNumber);
            syncMode = nsOptions.getStorageType() == StorageType.FILE_SYNC ? 
                                                    FileSegment.SyncMode.Sync : FileSegment.SyncMode.NoSync;
            fileSegment = FileSegment.openForRecovery(nsDir, segmentNumber, 
					            						nsOptions.getSegmentSize(), syncMode, 
					            						nsOptions);
            fileSegment.addReference();
            dsWalker = new DataSegmentWalker(fileSegment.dataBuf);
            lastEntry = null;
            for (DataSegmentWalkEntry entry : dsWalker) {
                if (verbose) {
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
    
    
    /*
    FileSegment recoverPartialSegment(File nsDir, int segmentNumber) {
        try {
            DataSegmentWalker   dsWalker;
            boolean             hasNext;
            
            Log.warning("Recovering partial segment: ", segmentNumber);
            dsWalker = new DataSegmentWalker(FileSegment.getDataSegment(nsDir, segmentNumber));
            do {
                hasNext = dsWalker.next();
            } while(hasNext);
            return FileSegment.openForRecovery(nsDir, segmentNumber);
        } catch (IOException ioe) {
            Log.logErrorWarning(ioe);
            return null;
        }
    }
    */
    
    void readFullSegment(int segmentNumber) {
        Log.warning("Reading full segment: ", segmentNumber);
        try {
            FileSegment segment;
            NamespaceProperties nsProperties;
            NamespaceOptions    nsOptions;
            //DataSegmentWalker       dsWalker;
            
            nsProperties = NamespacePropertiesIO.read(nsDir);
            nsOptions = nsProperties.getOptions();
            segment = FileSegment.openReadOnly(nsDir, segmentNumber, nsOptions.getSegmentSize(), nsOptions, 
                                               BufferMode.PreRead);
            for (DHTKeyIntEntry entry : segment.getPKC()) {
                int     offset;
                
                offset = entry.getValue();
                if (offset < 0) {
                    OffsetList  offsetList;
                    
                    offsetList = segment.offsetListStore.getOffsetList(-offset);
                    for (int listOffset : offsetList) {
                        System.out.printf("%s\t%d\t%d\t%f\t*\n", entry, segment.getCreationTime(offset), segment.getVersion(listOffset), KeyUtil.keyEntropy(entry));
                        //nsStore.putSegmentNumberAndVersion(entry.getKey(), segmentNumber, segment.getVersion(listOffset));
                    }
                } else {
                    System.out.printf("%s\t%d\t%d\t%f\n", entry, segment.getCreationTime(offset), segment.getVersion(offset), KeyUtil.keyEntropy(entry));
                    //nsStore.putSegmentNumberAndVersion(entry.getKey(), segmentNumber, segment.getVersion(offset));
                }
            }
            
            //dsWalker = new DataSegmentWalker(segment.dataBuf);
            //for (DataSegmentWalkEntry entry : dsWalker) {
            //    nsStore.addToSizeStats(entry.getUncompressedLength(), entry.getCompressedLength());
            //}
            
            segment.close();
            Log.warning("Done reading full segment: ", segmentNumber);
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
	                	segmentReader.readFullSegment(segmentNumber);
	                } catch (Exception e) {
	                	System.out.printf("Exception reading full segment. Reading partial.\n");
	                    segment = segmentReader.readPartialSegment(segmentNumber);
	                    segment.persist();
	                }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
