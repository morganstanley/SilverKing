package com.ms.silverking.cloud.dht.daemon.storage.convergence;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.daemon.NodeRingMaster2;
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceStore;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupConnection;
import com.ms.silverking.cloud.dht.net.ProtoChecksumTreeMessageGroup;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.numeric.LongInterval;
import com.ms.silverking.numeric.NumUtil;
import com.ms.silverking.time.AbsMillisTimeSource;

/**
 * Handles requests for checksum trees from remote servers. Computes the checksum tree requested, 
 * and sends the tree to the remote requester.
 * 
 * As checksum tree computation is expensive, this class ensures that extraneous computation
 * is avoided.
 */
public class ChecksumTreeServer {
    private final long              ns;
    private final NamespaceStore    nsStore;
    private final AbsMillisTimeSource   absMillisTimeSource;
    private final NodeRingMaster2       ringMaster;
    private final Cache<ChecksumTreeKey,CTGAndLock>  checksumTreeGroups;
    
    private static final boolean    debug = false;
    private static final boolean    verbose = true;
    
    //private static final int    entriesPerNode = 2; // 2 is for testing only
    private static final int    entriesPerNode = 65536;
    private static final int    mapMaxSize = 2;
    private static final int    mapExpirationMinutes = 30;
    
    public ChecksumTreeServer(NamespaceStore nsStore, AbsMillisTimeSource absMillisTimeSource) {
        this.ns = nsStore.getNamespace();
        this.nsStore = nsStore;
        this.absMillisTimeSource = absMillisTimeSource;
        ringMaster = nsStore.getRingMaster();        
        checksumTreeGroups = CacheBuilder.newBuilder().maximumSize(mapMaxSize)
                             .expireAfterAccess(mapExpirationMinutes, TimeUnit.MINUTES).build();
    }
    
    private static class ChecksumTreeKey {
        private final long          dhtConfigVersion;
        private final RingIDAndVersionPair        ringIDAndVersionPair;
        private final LongInterval  versions;
        
        ChecksumTreeKey(long dhtConfigVersion, RingIDAndVersionPair ringIDAndVersionPair, LongInterval versions) {
            this.dhtConfigVersion = dhtConfigVersion;
            this.ringIDAndVersionPair = ringIDAndVersionPair;
            this.versions = versions;
        }
        
        @Override
        public int hashCode() {
            return NumUtil.longHashCode(dhtConfigVersion) ^ ringIDAndVersionPair.hashCode() ^ versions.hashCode();
        }
        
        @Override
        public boolean equals(Object other) {
            ChecksumTreeKey oKey;
            
            oKey = (ChecksumTreeKey)other;
            return dhtConfigVersion == oKey.dhtConfigVersion 
                    && ringIDAndVersionPair.equals(oKey.ringIDAndVersionPair) && versions.equals(oKey.versions);
        }
    }
    
    private static class CTGAndLock {
        private final Lock          lock;
        private ChecksumTreeGroup   ctg;
        
        CTGAndLock() {
            lock = new ReentrantLock();
        }
        
        public void setCTG(ChecksumTreeGroup ctg) {
            this.ctg = ctg;
        }
        
        ChecksumTreeGroup getCTG() {
            return ctg;
        }
        
        public void lock() {
            lock.lock();
        }
        
        public void unlock() {
            lock.unlock();
        }
    }
    
    //////////////////////////////////////////////////////////////////////
    // internal ChecksumTree generation related code
    
    public ChecksumNode getRegionChecksumTree_Local(ConvergencePoint cp, RingRegion region, 
                                                    LongInterval versions) {
        CTGAndLock      ctgAndLock;
        ChecksumTreeKey key;
    	ChecksumNode	checksumTree;
        
        key = new ChecksumTreeKey(cp.getDHTConfigVersion(), cp.getRingIDAndVersionPair(), versions);
        /*
        ctgAndLock = checksumTreeGroups.get(key);
        if (ctgAndLock == null) {
            CTGAndLock  prev;
            
            ctgAndLock = new CTGAndLock();
            prev = checksumTreeGroups.get(key);
            if (prev == null) {
                checksumTreeGroups.putIfAbsent(key, ctgAndLock);
            } else {
                ctgAndLock = prev;
            }
        }
        */
        
        try {
            ctgAndLock = checksumTreeGroups.get(key, new Callable<CTGAndLock>() {
              @Override
              public CTGAndLock call() {
                return new CTGAndLock();
              }
            });
        } catch (ExecutionException ee) {
            throw new RuntimeException(ee);
        }        
        
        // FUTURE - could get rid of this lock for most cases. useful only to allow waiting for creation
        ctgAndLock.lock();
        try {
            ChecksumTreeGroup   ctg;
            
            if (ctgAndLock.getCTG() == null) {
                ctg = computeChecksumTreeGroup(cp.getRingIDAndVersionPair(), versions);
                if (ctg != null) {
                    ctgAndLock.setCTG(ctg);
                } else {
                    return null;
                }
            }
            ctg = ctgAndLock.getCTG();
            checksumTree = ctg.getTreeRoot(region.getStart());
            return RegionTreePruner.prune(checksumTree, region);
        } catch (RuntimeException re) {
        	System.err.printf("RuntimeException in getRegionChecksumTree_Local(). cp %s region %s version %s\n", cp, region, versions);
        	throw re;
        } finally {
            ctgAndLock.unlock();
        }
    }
    
    private ChecksumTreeGroup computeChecksumTreeGroup(RingIDAndVersionPair ringIDAndVersion, LongInterval versions) {
        return computeChecksumTreeGroup(ringIDAndVersion, versions.getStart(), versions.getEnd());
    }
    
    /**
     * Computes a new ChecksumTreeGroup given a version. Uses the regions in the ringMaster.
     * @param ringID TODO
     * @param minVersion
     * @param maxVersion
     * @return
     */
    private ChecksumTreeGroup computeChecksumTreeGroup(RingIDAndVersionPair ringIDAndVersion, long minVersion, long maxVersion) {
        Collection<RingRegion>    regions;
        
        Log.info("computeChecksumTreeGroup ", maxVersion);
        if (debug) {
            System.out.printf("computeChecksumTreeGroup %x\t%s\n", ns, ringIDAndVersion);
        }
        regions = ringMaster.getRegions(ringIDAndVersion);
        if (regions != null) {
            if (debug) {
            	System.out.println();
            	for (RingRegion region : regions) {
            		System.out.printf("%s\n", region);
            	}
            	System.out.println();
            }
            return computeChecksumTreeGroup(regions, minVersion, maxVersion, false);
        } else {
            return null;
        }
    }
    
    private ChecksumTreeGroup computeChecksumTreeGroup(RingRegion region, long minVersion, long maxVersion) {
        return computeChecksumTreeGroup(ImmutableSet.of(region), minVersion, maxVersion, true);
    }
    /**
     * Computes a new ChecksumTreeGroup given a version. Uses the regions in the ringMaster.
     * @param ringID TODO
     * @param minVersion
     * @param maxVersion
     * @return
     */
    private ChecksumTreeGroup computeChecksumTreeGroup(Collection<RingRegion> regions, 
                                                long minVersion, long maxVersion, boolean allowRegionNotFound) {
        Log.info("computeChecksumTreeGroup w/ regions");
        if (debug) {
            System.out.printf("computeChecksumTreeGroup w/ regions\n");
        }
        if (regions != null) {
            nsStore.readLock();
            try {
                return TreeBuilder.build(regions, nsStore.keyAndVersionChecksumIterator(minVersion, maxVersion), entriesPerNode, 
                    nsStore.getTotalKeys(), absMillisTimeSource.absTimeMillis(), minVersion, maxVersion, allowRegionNotFound);
            } finally {
                nsStore.readUnlock();
            }
        } else {
            return null;
        }
    }
    
    //////////////////////////////////////////////////////////////////////
    // Handle incoming request for a convergence tree
    
    public void getChecksumTree(UUIDBase uuid, ConvergencePoint targetCP, ConvergencePoint sourceCP, 
                                MessageGroupConnection connection, byte[] originator, RingRegion region) {
        ChecksumNode    root;
        ProtoChecksumTreeMessageGroup   pmg;
        int bufferSize;
        ConvergencePoint    mixedCP;

        if (verbose || debug) {
            System.out.printf("getChecksumTree %x\t%s\t%s\t%s\t%s\n", ns, targetCP, sourceCP, region, uuid);
        }
        // FUTURE - support both min and max version
        
        // We create a mixedCP because we need to retrieve target data from the source regions, but
        // we need to use the target data version
        mixedCP = sourceCP.dataVersion(targetCP.getDataVersion());
        root = getRegionChecksumTree_Local(mixedCP, region, 
                                           new LongInterval(Long.MIN_VALUE, mixedCP.getDataVersion()));
        if (root == null) {
            if (verbose || debug) {
                System.out.printf("null root for %x\t%s\t%s\n", ns, mixedCP, region);
            }
        }
        if (debug) {
            System.out.println("computed:");
            System.out.println(root);
        }
        bufferSize = estimateBufferSize(root);
        pmg = null;
        do {
            try {
                pmg = new ProtoChecksumTreeMessageGroup(uuid, ns, targetCP, originator, root, bufferSize);
                //pmg = new ProtoChecksumTreeMessageGroup(uuid, ns, mixedCP, originator, root, bufferSize);
            } catch (BufferOverflowException bfe) {
                pmg = null;
                if (bufferSize == Integer.MAX_VALUE) {
                    throw new RuntimeException("Buffer limit reached");
                } else {
                    if (debug) {
                        System.out.printf("raising checksum tree buffer limit %s\n", uuid);
                    }
                    bufferSize = bufferSize << 1;
                    if (bufferSize < 0) {
                        bufferSize = Integer.MAX_VALUE;
                    }
                }
            }
        } while (pmg == null);
        try {
            MessageGroup    mg;
            
            mg = pmg.toMessageGroup();
            if (verbose || debug) {
                System.out.printf("Sending checksum tree %s to %s\n", uuid, connection.getRemoteIPAndPort());
            }
            if (debug) {
                System.out.printf("Sending checksum tree at %d %d\n",
                    absMillisTimeSource.absTimeMillis(),
                    mg.getDeadlineAbsMillis(absMillisTimeSource));
            }
            connection.sendAsynchronous(mg, mg.getDeadlineAbsMillis(absMillisTimeSource));
        } catch (IOException ioe) {
            Log.logErrorWarning(ioe);
        }
    }
    
    private static final int    bufferExtra = 1024;
    private static final int    bufferBytesPerKey = DHTKey.BYTES_PER_KEY;
        
    private int estimateBufferSize(ChecksumNode root) {
        return (root != null ? root.estimatedKeys() : 0) * bufferBytesPerKey + bufferExtra;
    }

}
