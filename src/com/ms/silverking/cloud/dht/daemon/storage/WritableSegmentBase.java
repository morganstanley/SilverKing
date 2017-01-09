package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.ValueRetentionPolicy;
import com.ms.silverking.cloud.dht.ValueRetentionState;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.collection.DHTKeyIntEntry;
import com.ms.silverking.cloud.dht.collection.HybridPartialKeyCuckoo;
import com.ms.silverking.cloud.dht.collection.PartialKeyCuckoo;
import com.ms.silverking.cloud.dht.collection.PartialKeyIntCuckooBase;
import com.ms.silverking.cloud.dht.collection.TableFullException;
import com.ms.silverking.cloud.dht.collection.WritableCuckooConfig;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.MetaDataUtil;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.SimpleValueCreator;
import com.ms.silverking.cloud.dht.daemon.NodeRingMaster2;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.log.Log;
import com.ms.silverking.text.StringUtil;
import com.ms.silverking.util.ArrayUtil;

abstract class WritableSegmentBase extends AbstractSegment implements ReadableWritableSegment  {
    private final AtomicInteger nextFree;
    protected final int         dataSegmentSize;
    protected final int         indexOffset;
    
    protected PartialKeyIntCuckooBase  pkc;
    protected final int               segmentNumber; // zero-based
    protected final File          nsDir;
        
    protected static final int    dataOffset = SegmentFormat.headerSize;
    
    private static final boolean    debug = false;
    private static final boolean    debugCompaction = false;
    
    WritableSegmentBase(ByteBuffer dataBuf, OffsetListStore offsetListStore, 
                            int segmentNumber, PartialKeyIntCuckooBase pkc, int dataSegmentSize) {
        super(dataBuf, offsetListStore);
        this.segmentNumber = segmentNumber;
        this.pkc = pkc;
        nextFree = new AtomicInteger(SegmentFormat.headerSize);        
        this.nsDir = null;
        this.dataSegmentSize = dataSegmentSize;
        this.indexOffset = dataSegmentSize;
    }
    
    // called from openReadOnly
    WritableSegmentBase(File nsDir, int segmentNumber, ByteBuffer dataBuf, ByteBuffer htBuf,
            WritableCuckooConfig cuckooConfig, BufferOffsetListStore bufferOffsetListStore, 
            int dataSegmentSize) throws IOException {
        super(dataBuf, bufferOffsetListStore);        
        this.segmentNumber = segmentNumber;
        this.pkc = new PartialKeyCuckoo(cuckooConfig, this, htBuf);
        nextFree = new AtomicInteger(SegmentFormat.headerSize);
        this.nsDir = nsDir;
        this.dataSegmentSize = dataSegmentSize;
        this.indexOffset = dataSegmentSize;
        if (debug) {
            Log.warning("WritableSegmentBase created for read only: ", nsDir);
        }
    }    

    // called from Create
    WritableSegmentBase(File nsDir, int segmentNumber, ByteBuffer dataBuf, WritableCuckooConfig initialCuckooConfig, 
            int dataSegmentSize, NamespaceOptions nsOptions) {
        super(dataBuf, new RAMOffsetListStore(nsOptions));
        this.segmentNumber = segmentNumber;
        this.pkc = new HybridPartialKeyCuckoo(initialCuckooConfig, this);
        nextFree = new AtomicInteger(SegmentFormat.headerSize);
        this.nsDir = nsDir;
        this.dataSegmentSize = dataSegmentSize;
        this.indexOffset = dataSegmentSize;
        if (debug) {
            Log.warning("WritableSegmentBase created for writing/reading: ", nsDir);
        }
    }
    
    // FIXME - think about this def and usage
    public int totalSize() {
        return dataSegmentSize;
        //return data.length + pkc.getTotalSizeBytes();
    }
    
    int getSegmentNumber() {
        return segmentNumber;
    }
    
    public PartialKeyIntCuckooBase getPKC() {
        return pkc;
    }
    
    @Override
    protected int getRawOffset(DHTKey key) {
        return pkc.get(key);
    }    
    
    void setNextFree(int nextFree) {
        try {
            dataBuf.position(nextFree);
        } catch (IllegalArgumentException iae) {
            Log.warning(dataBuf.toString() +"\t"+ nextFree);
            throw iae;
        }
        this.nextFree.set(nextFree);
    }
    
    public SegmentStorageResult putFormattedValue(DHTKey key, ByteBuffer formattedBuf, StorageParameters storageParams, NamespaceOptions nsOptions) {
        int writeOffset;
        
        if (debugPut) {
            Log.warning("putFormattedBuf: ", KeyUtil.keyToString(key) +"\t"+ storageParams);
        }
        writeOffset = StorageFormat.writeFormattedValueToBuf(key, formattedBuf, dataBuf, nextFree, dataSegmentSize);
        if (writeOffset != StorageFormat.writeFailedOffset) {
            return _put(key, writeOffset, storageParams.getVersion(), storageParams.getValueCreator(), nsOptions);
        } else {
            return SegmentStorageResult.segmentFull;
        }
    }
    
    public SegmentStorageResult put(DHTKey key, ByteBuffer value, StorageParameters storageParams, byte[] userData, 
            NamespaceOptions nsOptions) {
        int writeOffset;
        
        if (debugPut) {
            Log.warning("put: ", KeyUtil.keyToString(key) +"\t"+ storageParams);
        }
        writeOffset = StorageFormat.writeToBuf(key, value, storageParams, userData, dataBuf, nextFree, dataSegmentSize);
        if (writeOffset != StorageFormat.writeFailedOffset) {
            return _put(key, writeOffset, storageParams.getVersion(), storageParams.getValueCreator(), nsOptions);
        } else {
            return SegmentStorageResult.segmentFull;
        }
    }
    
    public SegmentStorageResult _put(DHTKey key, int offset, long version, byte[] valueCreator, 
    								NamespaceOptions nsOptions) {
        OffsetList  offsetList;
        int existingOffset;
                    
        existingOffset = pkc.get(key);
        if (debugPut) {
            Log.warning("segmentNumber: ", segmentNumber);
            Log.warning("existingOffset: ", existingOffset);
        }
        if (existingOffset == PartialKeyIntCuckooBase.keyNotFound) {
            // no offset for the key; add the mapping
            if (debugPut) {
                Log.warning("initial mapping: ", KeyUtil.keyToString(key));
            }
            try {
                pkc.put(key, offset);
                if (debugPut) {
                    if (pkc.get(key) != offset) {
                        Log.warning("sanity check failed"+ pkc.get(key) +" "+ offset);
                    }
                }
            } catch (TableFullException tfe) {
                //tfe.printStackTrace();
                Log.warning("Segment pkc full. Creating new table");
                pkc = HybridPartialKeyCuckoo.rehashAndAdd(pkc, key, offset);
            }
        } else {
            // this key exists in pkc, we next determine whether it has
            // a single value associated with it or an offset list
            if (nsOptions.getVersionMode() == NamespaceVersionMode.SINGLE_VERSION) {
                //long    existingVersion;
                
                //existingVersion = getVersion(existingOffset);
                //if (version != existingVersion) {
                //    return SegmentStorageResult.invalidVersion;
                //} else {
                    byte[]  existingChecksum;
                    byte[]  newChecksum;
                    
                    existingChecksum = getChecksum(existingOffset);
                    newChecksum = getChecksum(offset);
                    if (ArrayUtil.compare(existingChecksum, newChecksum, ArrayUtil.MismatchedLengthMode.Ignore) == 0) {
                        return SegmentStorageResult.stored;
                    } else {
                        if (debugPut) {
                        	Log.warning(String.format("Checksums failed to compare: %s %s", 
                        			StringUtil.byteArrayToHexString(existingChecksum), StringUtil.byteArrayToHexString(newChecksum)));
                        }
                        return SegmentStorageResult.mutation;
                    }
                //}
            } else {
                if (existingOffset >= 0) {
                    long    existingVersion;
                    long    existingCreationTime;
                    long    creationTime;
                    
                    if (debugPut) {
                        Log.warning("single key associated: ", KeyUtil.keyToString(key));
                    }
                    // single key is associated, create an offset list
                    offsetList = offsetListStore.newOffsetList();                    
                    existingVersion = getVersion(existingOffset);
                    
                    if (nsOptions.getRevisionMode() == RevisionMode.UNRESTRICTED_REVISIONS) {
                        existingCreationTime = getCreationTime(offset);
                        creationTime = getCreationTime(offset);
                    } else {
                        existingCreationTime = 0;
                        creationTime = 0;
                    }
                    
                    if (nsOptions.getRevisionMode() == RevisionMode.UNRESTRICTED_REVISIONS
                            || version > existingVersion) {
                        offsetList.putOffset(existingVersion, existingOffset, existingCreationTime);                    
                        offsetList.putOffset(version, offset, creationTime);
                        if (debugPut) {
                            Log.warning("removing existing mapping: ", KeyUtil.keyToString(key));
                        }
                        boolean removed;
                        removed = pkc.remove(key);
                        if (debugPut || Log.levelMet(Level.FINE)) {
                            Log.warning("removed: ", removed);
                            Log.warning("pkc.get: ", pkc.get(key));
                            Log.warning("putting new mapping: ", KeyUtil.keyToString(key) +" "+ -((RAMOffsetList)offsetList).getIndex());
                        }
                        pkc.put(key, -((RAMOffsetList)offsetList).getIndex());
                    } else {
                    	ValueCreator	creator;
                    	
                		// FUTURE - Think about this. Important currently to allow for retries to succeed cleanly.
                    	creator = getCreator(offset);	
                    	if (SimpleValueCreator.areEqual(creator.getBytes(), valueCreator)) {
                            byte[]  existingChecksum;
                            byte[]  newChecksum;
                            
                            existingChecksum = getChecksum(existingOffset);
                            newChecksum = getChecksum(offset);
                            if (ArrayUtil.compare(existingChecksum, newChecksum) == 0) {
                            	//Log.warningf("pkc.getTotalEntries() %d", pkc.getTotalEntries());
                            	//Log.warningf("%s %d %d", key, existingOffset, offset);
                            	//Log.warningf("%s %d %d", key, existingVersion, version);
                        		return SegmentStorageResult.duplicateStore;
                            } else {
                                if (debugPut) {
                                	Log.warning(String.format("Duplicate version, but checksums failed to compare: %s %s", 
                                			StringUtil.byteArrayToHexString(existingChecksum), StringUtil.byteArrayToHexString(newChecksum)));
                                }
    	                        return SegmentStorageResult.invalidVersion;
                            }
                    	} else {
	                        // FUTURE: Consider: allow puts of incomplete stores to continue?
	                        if (debugPut) {
	                            Log.warning("WritableSegmentBase._put detected invalid version");
	                            Log.warning(nsOptions);
	                        }
	                        return SegmentStorageResult.invalidVersion;
                    	}
                    }
                } else {                    
                    if (debugPut) {
                        Log.warning("list associated: ", KeyUtil.keyToString(key));
                    }
                    // offset list is associated, use the existing offset list
                    offsetList = offsetListStore.getOffsetList(-existingOffset);
                    if (nsOptions.getRevisionMode() == RevisionMode.UNRESTRICTED_REVISIONS
                            || version > offsetList.getLatestVersion()) {
                        long    creationTime;
                        
                        if (debugPut || Log.levelMet(Level.FINE)) {
                            Log.warning("adding offset: ", KeyUtil.keyToString(key) +" "+ offset);
                        }
                        
                        if (nsOptions.getRevisionMode() == RevisionMode.UNRESTRICTED_REVISIONS) {
                            creationTime = getCreationTime(offset);
                        } else {
                            creationTime = 0;
                        }
                        offsetList.putOffset(version, offset, creationTime);                    
                    } else {
                        return SegmentStorageResult.invalidVersion;
                    }
                }
            }
        }
        
        // sanity check for debugging only
        //if (pkc.get(key) != offset) {
        //    throw new RuntimeException("failed sanity check");
        //}            
        return SegmentStorageResult.stored;
    }
    
    public abstract void persist() throws IOException;

    public OpResult putUpdate(DHTKey key, long version, byte storageState) {
        int offset;
        
        if (debugPut) {
            Log.warning("putUpdate: ", KeyUtil.keyToString(key) +"\t"+ version +"\t"+ storageState);
        }
        offset = getResolvedOffset(key, VersionConstraint.exactMatch(version));
        if (offset == noSuchKey) {
            Log.warning("putUpdate couldn't getResolvedOffset: ", KeyUtil.keyToString(key));
            return OpResult.ERROR;
        } else {
            offset += DHTKey.BYTES_PER_KEY;
            MetaDataUtil.updateStorageState(dataBuf, offset, storageState);
            return OpResult.SUCCEEDED;
        }
    }
    
	/////////////////////////

	public <T extends ValueRetentionState> Triple<CompactionCheckResult,Set<Integer>,Set<Integer>> 
							singleReverseSegmentWalk(ValueRetentionPolicy<T> vrp, T valueRetentionState, long curTimeNanos, NodeRingMaster2 ringMaster) {
		int		numRetained;
		int		numDiscarded;
		Set<Integer>	retainedOffsets;
		Set<Integer>	discardedOffsets;
		
		numRetained = 0;
		numDiscarded = 0;
		retainedOffsets = new HashSet<>();
		discardedOffsets = new HashSet<>();
		// Within each segment, the backwards walking is per key.
		// The outer loop loops through the keys first.
		for (DHTKeyIntEntry entry : pkc) {
			int					rawOffset;
			List<Integer>		offsetList;
			
			rawOffset = entry.getValue();
			if (rawOffset >= 0) {
				offsetList = ImmutableList.of(rawOffset);
			} else {
				offsetList = new ArrayList(ImmutableList.copyOf(offsetListStore.getOffsetList(-rawOffset)));
				Collections.sort(offsetList, Collections.reverseOrder());
			}
			// List is now in reverse order; iterate down through the offsets
			for (int offset : offsetList) {
				DHTKey	entryKey;
				
				entryKey = entry.getKey();
				//Log.warningf("%s %d %d %d %s", entry.getKey(), offset, getCreationTime(offset), curTimeNanos, isInvalidated(offset));
				if (vrp.retains(entryKey, getVersion(offset), getCreationTime(offset), isInvalidated(offset), valueRetentionState, curTimeNanos)
						&& ringMaster.iAmPotentialReplicaFor(entryKey)) {
					++numRetained;
					retainedOffsets.add(offset);
					//Log.warningf("Retained %s\t%d", entry.getKey(), offset);
				} else {
					++numDiscarded;
					discardedOffsets.add(offset);
					//Log.warningf("Discarded %s\t%d", entry.getKey(), offset);
				}
			}
		}
		return new Triple<>(new CompactionCheckResult(numRetained, numDiscarded), retainedOffsets, discardedOffsets);
	}
}
