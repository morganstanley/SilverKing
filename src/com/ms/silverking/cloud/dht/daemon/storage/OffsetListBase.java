package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.Iterator;

import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.log.Log;
import com.ms.silverking.numeric.NumConversion;

/**
 * List of offsets used in cases where we have multiple values stored for a particular key.
 * Extended concretely by RAMOffsetList - used in non-persisted segments - and
 * BufferOffsetList - for persisted segments.
 */
abstract class OffsetListBase implements OffsetList {
    protected final boolean   supportsStorageTime;
    protected final int       entrySizeInts;
    private final int       entrySizeBytes;
    
    protected static final int   indexOffset = 0;
    protected static final int   listSizeOffset = indexOffset + NumConversion.BYTES_PER_INT;
    
    protected static final int   persistedHeaderSizeInts = 2;
    protected static final int   persistedHeaderSizeBytes = persistedHeaderSizeInts * NumConversion.BYTES_PER_INT;
    
    protected static final int   entrySizeInts_noStorageTime = 3;
    protected static final int   entrySizeInts_supportsStorageTime = 5;
    protected static final int   versionOffset = 0;
    protected static final int   offsetOffset = 2;
    protected static final int   storageTimeOffset = 3;

    protected static final boolean  debug = false;    
    
    /*
     * Note: Care must be taken when working with sizes and indices. There are three units that are 
     * used in the appropriate places: entries, ints, and bytes. 
     */
    
    
    /*
     * Persisted format:
     * 
     * List:
     *      header
     *      entries...
     *      
     * Header:
     *      index                   4 bytes
     *      list size in entries    4 bytes
     * 
     * Entry (no storage time support):
     *      version 8 bytes
     *      offset  4 bytes
     *      
     * Entry (supportsStorageTime):
     *      version     8 bytes
     *      offset      4 bytes
     *      storageTime 8 bytes
     *      
     */
    
    protected OffsetListBase(boolean supportsStorageTime) {
        if (debug) {
            Log.warning("supportsStorageTime: ", supportsStorageTime);
        }
        this.supportsStorageTime = supportsStorageTime;
        if (supportsStorageTime) {
            entrySizeInts = entrySizeInts_supportsStorageTime;
        } else {
            entrySizeInts = entrySizeInts_noStorageTime;
        }
        entrySizeBytes = entrySizeInts * NumConversion.BYTES_PER_INT;
    }
    
    
    //public int getIndex() {
    //    return buf.getInt(indexOffset);
    //}
    
    protected int entryBaseOffset(int index) {
    	if (index < 0) {
    		Log.warningf("index %d", index);
    		throw new RuntimeException("index < 0");
    	}
        return index * entrySizeInts;
    }
    
    protected abstract long getVersion(int index);
    protected abstract int getOffset(int index);
    protected abstract long getStorageTime(int index);
    
    @Override
    public abstract void putOffset(long version, int offset, long storageTime);

    private static final int    linearThreshold = 2;
    
    /*
     * for newest:
     * binary search to find the max
     * 
     * for oldest:
     * binary search to find the oldest
     * 
     * special case for the most recent
     * 
     */
    
    @Override
    public int getOffset(VersionConstraint vc, ValidityVerifier validityVerifier) {
        // FUTURE - improve efficiency 
        if (vc.equals(VersionConstraint.greatest) && !supportsStorageTime && validityVerifier == null) {
            return getLastOffset();
        } else {
            return getOffset_linear(vc, validityVerifier);
        }
    }
    
    private int getOffset_linear(VersionConstraint vc, ValidityVerifier validityVerifier) {
        long    bestMatchVersion;
        int     bestMatchIndex;

        bestMatchIndex = Integer.MIN_VALUE;
        if (vc.getMode() == VersionConstraint.Mode.GREATEST) {
            bestMatchVersion = Long.MIN_VALUE;
        } else {
            bestMatchVersion = Long.MAX_VALUE;
        }
        if (debug) {
            Log.warning("getOffset: ", vc);
        }
        // FUTURE - replace linear search
        for (int i = 0; i < size(); i++) {
            long    curVersion;
            
            // StorageTimes are increasing. Exit this loop if we have exceeded the maxStorageTime.
            if (debug) {
                if (supportsStorageTime) {
                    Log.warning("vc.getMaxStorageTime()\t"+ vc.getMaxCreationTime() 
                        +" getStorageTime(i) "+ getStorageTime(i));
                }
            }
            if (supportsStorageTime 
                    && vc.getMaxCreationTime() != VersionConstraint.noCreationTimeLimit
                    && vc.getMaxCreationTime() < getStorageTime(i)) {
                if (debug) {
                    Log.warning("vc.getMaxStorageTime() < getStorageTime(i)\t", 
                        vc.getMaxCreationTime() +" < "+ getStorageTime(i));
                }
                break;
            }
            
            if (debug) {
                Log.warning(i +"\t"+ vc +"\t"+ getVersion(i));
                displayEntry(i);
            }
            curVersion = getVersion(i);
            assert curVersion >= 0;
            if (vc.matches(curVersion)) {
                if (vc.getMode() == VersionConstraint.Mode.LEAST) {
                    if (curVersion <= bestMatchVersion) { 
                    	if (validityVerifier == null || validityVerifier.isValid(getOffset(i) + DHTKey.BYTES_PER_KEY)) {
	                        bestMatchIndex = i;
	                        bestMatchVersion = curVersion;
	                        if (debug) {
	                            Log.warning("found new least: ", i +" "+ curVersion +" "+ bestMatchVersion);
	                        }
                    	}
                    }
                } else {
                    if (curVersion >= bestMatchVersion) {                    
                    	if (validityVerifier == null || validityVerifier.isValid(getOffset(i) + DHTKey.BYTES_PER_KEY)) {
	                        bestMatchIndex = i;
	                        bestMatchVersion = curVersion;
	                        if (debug) {
	                            Log.warning("found new greatest: ", i +" "+ curVersion +" "+ bestMatchVersion);
	                        }
                    	}
                    }
                }
            }
        }
        if (bestMatchIndex < 0) {
            if (debug) {
                Log.warning("not found");
            }
            return NO_MATCH_FOUND;
        } else {
            if (debug) {
                Log.warning("found greatest match: ", bestMatchIndex);
            }
            return getOffset(bestMatchIndex);
        }
    }
    
	@Override
    public int getFirstOffset() {
        return getOffset(0);
    }
    
    @Override
    public int getLastOffset() {
    	int	lastIndex;
    	
    	lastIndex = lastIndex();
    	if (lastIndex >= 0) {
    		return getOffset(lastIndex);
    	} else {
    		return -1;
    	}
    }
    
    @Override
    public long getLatestVersion() {
    	int	lastIndex;
    	
    	lastIndex = lastIndex();
    	if (lastIndex >= 0) {
    		return getVersion(lastIndex);
    	} else {
    		return -1;
    	}
    }
    
    protected void checkIndex(int index) {
		if (index < 0) {
			throw new IndexOutOfBoundsException(index +" < 0");
		}
		if (index >= size()) {
			throw new IndexOutOfBoundsException(index +" index >= "+ size());
		}        
    }    
    
    private int lastIndex() {
        return size() - 1;
    }
    
    private void displayEntry(int i) {
        System.out.printf("%d\t%d\n", getVersion(i), getOffset(i));
    }
    
    @Override
    public void displayForDebug() {
        System.out.println("*** list start *** "+ size());
        for (int i = 0; i < size(); i++) {
            displayEntry(i);
        }
        System.out.println("*** list end ***\n");
    }
    
    @Override
    public Iterator<Integer> iterator() {
        return new OffsetListIterator();
    }
    
    protected abstract class OffsetListIteratorBase<T> implements Iterator<T> {
        protected int index;
        
        OffsetListIteratorBase() {
        }

        @Override
        public boolean hasNext() {
            return index <= lastIndex();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
    
    private class OffsetListIterator extends OffsetListIteratorBase<Integer> {
        OffsetListIterator() {
        }

        @Override
        public Integer next() {
            if (hasNext()) {
                return getOffset(index++);
            } else {
                return null;
            }
        }
    }
    
    @Override
    public Iterable<Long> versionIterable() {
    	return new VersionIterator();
    }
    
    @Override
    public Iterator<Long> versionIterator() {
        return new VersionIterator();
    }
    
    private class VersionIterator extends OffsetListIteratorBase<Long> implements Iterable<Long> {
        VersionIterator() {
        }

        @Override
        public Long next() {
            if (hasNext()) {
                return getVersion(index++);
            } else {
                return null;
            }
        }

		@Override
		public Iterator<Long> iterator() {
			return this;
		}
    }

    @Override
    public Iterable<Pair<Long,Long>> versionAndStorageTimeIterable() {
    	return new VersionAndStorageTimeIterator();
    }
    
    @Override
    public Iterator<Pair<Long,Long>> versionAndStorageTimeIterator() {
        return new VersionAndStorageTimeIterator();
    }
    
    private class VersionAndStorageTimeIterator extends OffsetListIteratorBase<Pair<Long,Long>> implements Iterable<Pair<Long, Long>> {
        VersionAndStorageTimeIterator() {
        }

        @Override
        public Pair<Long,Long> next() {
            if (hasNext()) {
            	Pair<Long,Long>	p;
            	
            	p = new Pair<>(getVersion(index), getStorageTime(index));
            	index++;
                return p;
            } else {
                return null;
            }
        }

		@Override
		public Iterator<Pair<Long, Long>> iterator() {
			return this;
		}
    }    

    @Override
    public Iterable<Triple<Integer,Long,Long>>	offsetVersionAndStorageTimeIterable() {
    	return new OffsetVersionAndStorageTimeIterator();
    }
    
    @Override
    public Iterator<Triple<Integer,Long,Long>> offsetVersionAndStorageTimeIterator() {
        return new OffsetVersionAndStorageTimeIterator();
    }
    
    private class OffsetVersionAndStorageTimeIterator extends OffsetListIteratorBase<Triple<Integer,Long,Long>> implements Iterable<Triple<Integer, Long, Long>> {
    	OffsetVersionAndStorageTimeIterator() {
        }

        @Override
        public Triple<Integer,Long,Long> next() {
            if (hasNext()) {
            	Triple<Integer,Long,Long>	t;
            	
            	t = new Triple<>(getOffset(index), getVersion(index), supportsStorageTime ? getStorageTime(index) : 0);
            	index++;
                return t;
            } else {
                return null;
            }
        }

		@Override
		public Iterator<Triple<Integer, Long, Long>> iterator() {
			return this;
		}
    }    
    
    @Override
    public MultiVersionChecksum getMultiVersionChecksum() {
    	MultiVersionChecksum	mvc;
    	
    	mvc = new MultiVersionChecksum();
    	for (int i = 0; i < size(); i++) {
    		mvc.addVersionAndStorageTime(getVersion(i), supportsStorageTime ? getStorageTime(i) : 0);
    	}
    	return mvc;
    }
    
    public static int entrySizeBytes(boolean supportsStorageTime) {
        return (supportsStorageTime ? entrySizeInts_supportsStorageTime : entrySizeInts_noStorageTime) 
                * NumConversion.BYTES_PER_INT;
    }
}
