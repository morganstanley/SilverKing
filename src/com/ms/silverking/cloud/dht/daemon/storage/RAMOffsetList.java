package com.ms.silverking.cloud.dht.daemon.storage;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Set;
import java.util.logging.Level;

import com.ms.silverking.collection.Triple;
import com.ms.silverking.log.Log;
import com.ms.silverking.numeric.NumConversion;

/**
 * Readable/writable OffsetList stored in RAM. 
 */
class RAMOffsetList extends OffsetListBase {
    private final int       index;
    private final IntList   offsetList;

    // this code presumes LITTLE_ENDIAN
    // FUTURE - support BIG_ENDIAN as well
    private static final int   lsiOffset = 0;
    private static final int   msiOffset = 1;
    private static final int   storageTimeLsiOffset = offsetOffset + 1 + lsiOffset;
    private static final int   storageTimeMsiOffset = offsetOffset + 1 + msiOffset;
    
    private static final int	intArrayListDefaultInitialSize = 1;
    
    static {
        if (ByteOrder.nativeOrder() != ByteOrder.LITTLE_ENDIAN) {
            throw new RuntimeException("Only LITTLE_ENDIAN implemented in this class");
        }
    }
    
    RAMOffsetList(int index, boolean supportsStorageTime) {
        super(supportsStorageTime);
        this.index = index;
        offsetList = new IntArrayList(intArrayListDefaultInitialSize);
    }
    
    public int getIndex() {
        return index;
    }
    
    public int firstIndexOfOffset(int offset) {
    	int	s;
    	
    	s = size();
    	for (int i = 0; i < s; i++) {
    		if (getOffset(i) == offset) {
    			return i;
    		}
    	}
    	return -1;
    }
    
	public void removeEntryAt(int index) {
        int baseOffset;

        checkIndex(index);
        baseOffset = entryBaseOffset(index);
		offsetList.removeElements(baseOffset, baseOffset + entrySizeInts);
	}    
    
	public void removeEntries(int minRemovalIndex, int maxRemovalIndex) {
        int baseOffset0;
        int baseOffset1;

        checkIndex(minRemovalIndex);
        checkIndex(maxRemovalIndex);
        baseOffset0 = entryBaseOffset(minRemovalIndex);
        baseOffset1 = entryBaseOffset(maxRemovalIndex);
        offsetList.removeElements(baseOffset0, baseOffset1 + entrySizeInts);
	}
	
	public int getNumEntries() {
		return offsetList.size() / entrySizeInts;
	}
	
	public void removeEntriesByValue(Set<Integer> valuesToRemove) {
		int	numEntries;
		
		numEntries = getNumEntries();
		for (int i = numEntries - 1; i >= 0; i--) {
			int	offset;
			
			offset = getOffset(i);
			if (valuesToRemove.contains(offset)) {
				removeEntryAt(i);
			}
		}
	}
	
	public void removeEntriesByMatch(Set<Triple<Long,Integer,Long>> entriesToRemove) {
		int	numEntries;
		
		numEntries = getNumEntries();
		for (int i = numEntries - 1; i >= 0; i--) {
			Triple<Long,Integer,Long>	listEntry;
			
			listEntry = new Triple<>(getVersion(i), getOffset(i), supportsStorageTime ? getStorageTime(i) : 0);
			if (entriesToRemove.contains(listEntry)) {
				removeEntryAt(i);
			}
		}
	}
	
    private void addEntry(int insertionIndex, long version, int offset, long storageTime) {
        int baseOffset;
        
        baseOffset = entryBaseOffset(insertionIndex);
        offsetList.add(baseOffset + lsiOffset, (int)(version & 0xffffffff));
        offsetList.add(baseOffset + msiOffset, (int)(version >>> 32));
        offsetList.add(baseOffset + offsetOffset, offset);
        if (supportsStorageTime) {
            offsetList.add(baseOffset + storageTimeLsiOffset, (int)(storageTime & 0xffffffff));
            offsetList.add(baseOffset + storageTimeMsiOffset, (int)(storageTime >>> 32));
        }
        if (debug || Log.levelMet(Level.FINE)) {
            Thread.dumpStack();
            Log.warning("RAMOffsetList.addEntry ", version +" "+ offset +" "+ storageTime);
        }
    }
        
    @Override
    protected long getVersion(int index) {
        int baseOffset;
        
        baseOffset = entryBaseOffset(index);
        return NumConversion.intsToLong(offsetList.get(baseOffset + msiOffset), offsetList.get(baseOffset + lsiOffset));
    }
    
    @Override
    protected long getStorageTime(int index) {
        int baseOffset;
        
        assert supportsStorageTime;
        
        baseOffset = entryBaseOffset(index);
        return NumConversion.intsToLong(offsetList.get(baseOffset + storageTimeMsiOffset), 
                                        offsetList.get(baseOffset + storageTimeLsiOffset));
    }
    
    @Override
    protected int getOffset(int index) {
        return offsetList.get(entryBaseOffset(index) + offsetOffset);
    }

    @Override
    public int size() {
        return offsetList.size() / entrySizeInts;
    }
    
    @Override
    public void putOffset(long version, int offset, long storageTime) {
        assert version >= 0;
        // FUTURE - ENFORCE LATEST VERSION?
        //System.out.println("\t\t\t\tlatestVersion(): "+ (size() > 0 ? latestVersion() : 0));
        addEntry(size(), version, offset, storageTime);
        //addEntry(getInsertionIndex(version), version, offset);
    }
    
    private long latestVersion() {
        return getVersion(size() - 1);
    }

    public void persist(ByteBuffer buf) {
        if (debug) {
            System.out.println("RAMOffsetList.persist");
            System.out.println("buf.order "+ buf.order());
            System.out.printf("%d\t%d\n", index, offsetList.size());
        }
        buf.putInt(index);
        buf.putInt(size());
        for (int x : offsetList) {
            if (debug) {
                System.out.printf("\t%d\n", x);
            }
            buf.putInt(x);
        }
    }

    public int persistedSizeBytes() {
        return (persistedHeaderSizeInts + offsetList.size()) * NumConversion.BYTES_PER_INT;
    }
}
