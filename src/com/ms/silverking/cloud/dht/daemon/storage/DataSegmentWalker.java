package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.MetaDataUtil;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.numeric.NumConversion;

/**
 * Walk through a segment's data segment directly without
 * looking at the PKC.
 */
public class DataSegmentWalker implements Iterator<DataSegmentWalkEntry>, Iterable<DataSegmentWalkEntry> {
    private final ByteBuffer    dataSegment;
    private int position;
    private boolean hasNext;
    
    private static final boolean    debug = false;
    
    private static final double minValidKeyEntropy = 2.5;
    
    public DataSegmentWalker(ByteBuffer dataSegment) {
        this.dataSegment = dataSegment;
        if (debug) {
            System.out.printf("%s\t%d\t%d\n", dataSegment, SegmentFormat.headerSize, MetaDataUtil.getMinimumEntrySize());
        }
        hasNext = (dataSegment.limit() >= SegmentFormat.headerSize + MetaDataUtil.getMinimumEntrySize());
        if (hasNext) {
            long    nextMSL;
            long    nextLSL;
            
            position = SegmentFormat.headerSize;
            nextMSL = dataSegment.getLong(position);
            nextLSL = dataSegment.getLong(position + NumConversion.BYTES_PER_LONG);
            hasNext = !(nextMSL == 0 && nextLSL == 0);
            if (debug) {
                System.out.printf("\t%s %d %x %x\n", hasNext, position, nextMSL, nextLSL);
            }
        }
    }
    
    @Override
    public boolean hasNext() {
        return hasNext;
    }
    
    /**
     * Position at next entry. Initial call
     * @return if there is an additional entry
     */
    @Override
    public DataSegmentWalkEntry next() {
        long    msl;
        long    lsl;
        int     storedLength;
        int     uncompressedLength;
        int     compressedLength;
        long    nextMSL;
        long    nextLSL;
        
        if (!hasNext) {
            return null;
        } else {
            DHTKey  curKey;
            int     curOffset;
            long    version;
            long    creationTime;
            //double  keyEntropy;
            byte	storageState;
            ValueCreator	creator;
            ByteBuffer	entry;
            int	nextEntryPostKeyPosition;
            
            if (debug) {
                System.out.println("position: "+ position);
            }
            msl = dataSegment.getLong(position);
            lsl = dataSegment.getLong(position + NumConversion.BYTES_PER_LONG);
            
            curOffset = position;
            position += DHTKey.BYTES_PER_KEY;
            curKey = new SimpleKey(msl, lsl);
            
            // FUTURE - probably delete the below code, but could consider some
            // additional sanity checking
            /*
            keyEntropy = KeyUtil.keyEntropy(curKey);            
            if (keyEntropy < minValidKeyEntropy) {
                boolean sane;
                
                Log.warning("Invalid key: ", curKey +" "+ position);
                sane = false;
                // FUTURE - need more sophisticated validation of entry
                // FUTURE - we need to decode the entry
                while (!sane) {
                    if (position > dataSegment.limit() - DHTKey.BYTES_PER_KEY) { // FUTURE - crude limit; refine
                        Log.warning("Ran off the end of the segment searching for a valid key");
                        return null;
                    }
                    position++;
                    msl = dataSegment.getLong(position);
                    lsl = dataSegment.getLong(position + NumConversion.BYTES_PER_LONG);
                    curKey = new SimpleKey(msl, lsl);
                    keyEntropy = KeyUtil.keyEntropy(curKey);

                    if (keyEntropy >= minValidKeyEntropy) {
                        try {
                            storedLength = MetaDataUtil.getStoredLength(dataSegment, position);
                            if (storedLength > 0) {
                                uncompressedLength = MetaDataUtil.getUncompressedLength(dataSegment, position);
                                if (uncompressedLength >= 0) {
                                    compressedLength = MetaDataUtil.getCompressedLength(dataSegment, position);
                                    if (compressedLength >= 0) {
                                        if (position + storedLength < dataSegment.limit() 
                                                && uncompressedLength < compressedLength) {
                                            sane = true;
                                        }
                                    }
                                }
                            }
                        } catch (Exception e) {
                            // couldn't decode entry, move to next
                        }
                    }
                }
            }
            
            if (debug) {
                System.out.printf("%x:%x %3.2f\n", msl, lsl, keyEntropy);
            }
            */
            
            storedLength = MetaDataUtil.getStoredLength(dataSegment, position);
            uncompressedLength = MetaDataUtil.getUncompressedLength(dataSegment, position);
            compressedLength = MetaDataUtil.getCompressedLength(dataSegment, position);
            version = MetaDataUtil.getVersion(dataSegment, position);
            creationTime = MetaDataUtil.getCreationTime(dataSegment, position);
            storageState = MetaDataUtil.getStorageState(dataSegment, position);
            creator = MetaDataUtil.getCreator(dataSegment, position);
            if (debug) {
                System.out.println(storedLength);
            }
            nextEntryPostKeyPosition = position + storedLength + DHTKey.BYTES_PER_KEY; 
            // Check to see if it's possible that there is another entry
            if (nextEntryPostKeyPosition + MetaDataUtil.getMinimumEntrySize() < dataSegment.limit()) {
            	int	nextEntryStoredLength;
            	
            	// Check to see if the potential next entry actually fits in this segment
            	nextEntryStoredLength = MetaDataUtil.getStoredLength(dataSegment, nextEntryPostKeyPosition);
            	if (debug) {
            		System.out.printf("nextEntryPostKeyPosition %d nextEntryStoredLength %d dataSegment.limit() %d\n",
            				nextEntryPostKeyPosition, nextEntryStoredLength, dataSegment.limit());
            	}
            	if (nextEntryPostKeyPosition + nextEntryStoredLength < dataSegment.limit()) {            	            	
	                nextMSL = dataSegment.getLong(nextEntryPostKeyPosition);
	                nextLSL = dataSegment.getLong(nextEntryPostKeyPosition + NumConversion.BYTES_PER_LONG);
	                hasNext = !(nextMSL == 0 && nextLSL == 0);
	                if (debug) {
	                	System.out.printf("a: hastNext %s\n", hasNext);
	                }
            	} else {
            		hasNext = false;
	                if (debug) {
	                	System.out.printf("b: hastNext %s\n", hasNext);
	                }
            	}
            } else {
            	// No room for valid next entry
                hasNext = false; 
                if (debug) {
                	System.out.printf("c: hastNext %s\n", hasNext);
                }
            }
            
            entry = (ByteBuffer)((ByteBuffer)dataSegment.duplicate().position(position)).slice().limit(storedLength);
            
            position += storedLength;
            
            return new DataSegmentWalkEntry(curKey,
                                            version,
                                            curOffset,
                                            storedLength,
                                            uncompressedLength,
                                            compressedLength,
                                            DHTKey.BYTES_PER_KEY,
                                            entry,
                                            creationTime, 
                                            creator,
                                            storageState);
        }
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            if (args.length != 2) {
                System.out.println("args: <nsDir> <segmentNumber>");
                return;
            } else {
                ByteBuffer  dataBuf;
                File        nsDir;
                int         segmentNumber;
                DataSegmentWalker   dsWalker;
                NamespaceProperties nsProperties;
                NamespaceOptions    nsOptions;
                
                nsDir = new File(args[0]);
                segmentNumber = Integer.parseInt(args[1]);
                nsProperties = NamespacePropertiesIO.read(nsDir);
                nsOptions = nsProperties.getOptions();
                dataBuf = FileSegment.getDataSegment(nsDir, segmentNumber, nsOptions.getSegmentSize());
                dsWalker = new DataSegmentWalker(dataBuf);
                while (dsWalker.hasNext()) {
                    DataSegmentWalkEntry  entry;
                    
                    entry = dsWalker.next();
                    System.out.println(entry.getOffset() +" "+ entry);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<DataSegmentWalkEntry> iterator() {
        return this;
    }
}
