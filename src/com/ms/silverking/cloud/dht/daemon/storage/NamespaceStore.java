package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceServerSideCode;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.NonExistenceResponse;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.StorageType;
import com.ms.silverking.cloud.dht.ValueRetentionPolicy;
import com.ms.silverking.cloud.dht.ValueRetentionState;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.VersionConstraint.Mode;
import com.ms.silverking.cloud.dht.WaitMode;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.collection.DHTKeyIntEntry;
import com.ms.silverking.cloud.dht.collection.IntArrayCuckoo;
import com.ms.silverking.cloud.dht.collection.IntCuckooConstants;
import com.ms.silverking.cloud.dht.collection.TableFullException;
import com.ms.silverking.cloud.dht.collection.WritableCuckooConfig;
import com.ms.silverking.cloud.dht.common.CCSSUtil;
import com.ms.silverking.cloud.dht.common.CorruptValueException;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.DHTKeyComparator;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.common.KeyAndInteger;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.MetaDataConstants;
import com.ms.silverking.cloud.dht.common.MetaDataUtil;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.common.NamespaceUtil;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.OptionsHelper;
import com.ms.silverking.cloud.dht.common.SegmentIndexLocation;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.common.ValueUtil;
import com.ms.silverking.cloud.dht.daemon.ActiveProxyRetrieval;
import com.ms.silverking.cloud.dht.daemon.NodeRingMaster2;
import com.ms.silverking.cloud.dht.daemon.Waiter;
import com.ms.silverking.cloud.dht.daemon.storage.FileSegment.SegmentPrereadMode;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ActiveRegionSync;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ChecksumNode;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ChecksumTreeRequest;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ChecksumTreeServer;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ConvergencePoint;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.StorageProtocolUtil;
import com.ms.silverking.cloud.dht.meta.LinkCreationListener;
import com.ms.silverking.cloud.dht.meta.LinkCreationWatcher;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.net.MessageGroupConnection;
import com.ms.silverking.cloud.dht.net.MessageGroupKeyOrdinalEntry;
import com.ms.silverking.cloud.dht.net.MessageGroupRetrievalResponseEntry;
import com.ms.silverking.cloud.dht.serverside.PutTrigger;
import com.ms.silverking.cloud.dht.serverside.RetrieveTrigger;
import com.ms.silverking.cloud.dht.serverside.SSNamespaceStore;
import com.ms.silverking.cloud.dht.serverside.SSRetrievalOptions;
import com.ms.silverking.cloud.dht.serverside.SSStorageParameters;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.cloud.storagepolicy.StoragePolicy;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.collection.HashedSetMap;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.SKImmutableList;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.io.FileUtil;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.text.StringUtil;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;
import com.ms.silverking.time.SystemTimeSource;
import com.ms.silverking.util.PropertiesHelper;

public class NamespaceStore implements SSNamespaceStore {
    private final long ns;
    private NamespaceStore  parent;
    private final File nsDir;
    private final File ssDir;
    private final NamespaceProperties nsProperties;
    private final NamespaceOptions nsOptions;
    private final boolean	verifyStorageState;
    private final MessageGroupBase mgBase;
    private final NodeRingMaster2 ringMaster;
    private final ChecksumTreeServer    checksumTreeServer;
    private volatile WritableSegmentBase headSegment;
    //private final Lock headCreationLock;
    // private final ConcurrentMap<DHTKey,Integer> valueSegments;
    // maps DHTKeys to the segment that stores an existing entry for that key
    // if more than one entry exists, it will contain the index of the
    // OffsetList for the DHTKey
    /** 
     * Maps keys to:
     *  a) the segment where the value is stored (for single value storage)
     *  b) the list of segments where the value is stored
     */
    private IntArrayCuckoo valueSegments;
    private final AtomicInteger nextSegmentID;
    private final OffsetListStore offsetListStore;
    private final ReadWriteLock metaRWLock;
    private final Lock metaReadLock;
    private final Lock metaWriteLock;
    private final ReadWriteLock rwLock;
    private final Lock readLock;
    private final Lock writeLock;
    private long minVersion;
    private long curSnapshot;
    private final ConcurrentMap<UUIDBase, ActiveProxyRetrieval> activeRetrievals;
    private final ConcurrentMap<DHTKey, Set<PendingWaitFor>> pendingWaitFors;
    private final Map<Integer, FileSegment> recentFileSegments;
    private final Map<Integer, RAMSegment> ramSegments;
    private final NamespaceStats    nsStats;
    private long lastConvergenceVersion;
    private long lastConvergenceTotalKeys;
    protected final SystemTimeSource   systemTimeSource;
    private final Set<Integer>	deletedSegments;
    private final PutTrigger	putTrigger;
    private final RetrieveTrigger	retrieveTrigger;

    private final ConcurrentMap<UUIDBase,ActiveRegionSync>	activeRegionSyncs;    
    
    private static final byte[] emptyUserData = new byte[0];
    
    private static final boolean debug = false;
    private static final boolean debugConvergence = false || debug;
    private static final boolean debugParent = false || debug;
    private static final boolean debugVersion = false || debug;
    private static final boolean debugSegments = false || debug;
    private static final boolean debugWaitFor = false || debug;
    private static final boolean debugReap = false || debug;
    
    private static final int vsNumSubTables = 4;
    private static final int vsEntriesPerBucket = 4;
    private static final int vsTotalEntries = vsNumSubTables * vsEntriesPerBucket;
    private static final int vsCuckooLimit = 32;
    private static final WritableCuckooConfig valueSegmentsConfig = new WritableCuckooConfig(vsTotalEntries,
            vsNumSubTables, vsEntriesPerBucket, vsCuckooLimit);
    private static final InternalRetrievalOptions oldestVersionOptions = new InternalRetrievalOptions(
    		OptionsHelper.newRetrievalOptions(RetrievalType.META_DATA, WaitMode.GET, VersionConstraint.least));
    private static final InternalRetrievalOptions newestVersionOptions = new InternalRetrievalOptions(
    		OptionsHelper.newRetrievalOptions(RetrievalType.META_DATA, WaitMode.GET, VersionConstraint.greatest));
    private static final int fileSegmentCacheCapacity = StoreConfiguration.fileSegmentCacheCapacity;
    private static final long noSuchVersion = MetaDataConstants.noSuchVersion;

    private static final boolean    testStorageCorruption = false;
    private static final double     storageCorruptionProbability = 0.3;
    
    private static final int	fileSegmentAttempts = 10;
    private static final int	fileSegmentAttemptDelayMillis = 1 * 1000;
    
    private static final ByteBuffer	valueStorageStateInvalidForRead = ByteBuffer.allocate(0);
    
    // FUTURE - allow for a richer compaction policy
    private static final double	compactionThreshold = 0.1;
    
    private static final int	maxFailedStores = 1000000;
    
    public enum DirCreationMode {
        CreateNSDir, DoNotCreateNSDir
    };
    
    private enum VersionCheckResult {Invalid, Valid, Equal};
    
    private static final int    VERSION_INDEX = 0;
    private static final int    STORAGE_TIME_INDEX = 1;
    
    private static final SegmentIndexLocation segmentIndexLocation;
    private static final int	nsPrereadGB;
    private static final SegmentPrereadMode	readSegmentPrereadMode = SegmentPrereadMode.NoPreread;
    private static final SegmentPrereadMode	updateSegmentPrereadMode = SegmentPrereadMode.NoPreread;
    
    static {
    	segmentIndexLocation = SegmentIndexLocation.valueOf(PropertiesHelper.systemHelper.getString(DHTConstants.segmentIndexLocationProperty, DHTConstants.defaultSegmentIndexLocation.toString()));
    	Log.warningf("segmentIndexLocation: %s", segmentIndexLocation);
    	nsPrereadGB = PropertiesHelper.systemHelper.getInt(DHTConstants.nsPrereadGBProperty, DHTConstants.defaultNSPrereadGB);
    	Log.warningf("nsPrereadGB: %s", nsPrereadGB);
    }
    
    public NamespaceStore(long ns, File nsDir, DirCreationMode dirCreationMode, NamespaceProperties nsProperties,
            NamespaceStore parent,
            MessageGroupBase mgBase, NodeRingMaster2 ringMaster, boolean isRecovery,
            ConcurrentMap<UUIDBase, ActiveProxyRetrieval> activeRetrievals) {
        this.ns = ns;
        this.nsDir = nsDir;
        ssDir = new File(nsDir, DHTConstants.ssSubDirName);
        activeRegionSyncs = new ConcurrentHashMap<>();
        this.nsOptions = nsProperties.getOptions();
        verifyStorageState = StorageProtocolUtil.requiresStorageStateVerification(nsOptions.getConsistencyProtocol());
        this.nsProperties = nsProperties;
        this.parent = parent;
        this.mgBase = mgBase;
        this.ringMaster = ringMaster;
        checksumTreeServer = new ChecksumTreeServer(this, mgBase.getAbsMillisTimeSource());
        switch (dirCreationMode) {
        case CreateNSDir:
            if (!nsDir.mkdirs()) {
                if (!nsDir.exists()) {
                    throw new RuntimeException("Unable to make nsDir");
                } else {
                    Log.warning("nsDir creation ignored due to dir already exists");
                }
            }
            try {
                NamespacePropertiesIO.write(nsDir, nsProperties);
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
            break;
        case DoNotCreateNSDir:
            break;
        default:
            throw new RuntimeException("panic");
        }
        nextSegmentID = new AtomicInteger(0);
        switch (nsOptions.getStorageType()) {
        case RAM:
            ramSegments = new ConcurrentHashMap<>(); // FUTURE - consider a more efficient map
            recentFileSegments = null;
            break;
        case FILE_SYNC:
        case FILE:
            ramSegments = null;
            if (!FileSegment.mapEverything) {
            	recentFileSegments = new FileSegmentMap(fileSegmentCacheCapacity);
            } else {
            	recentFileSegments = new ConcurrentHashMap<>();
            }
            break;
        default: throw new RuntimeException("Panic");
        }
        if (!isRecovery) {
        	createInitialHeadSegment();
        }
        //headCreationLock = new ReentrantLock();
        // valueSegments = new ConcurrentHashMap<>();
        valueSegments = new IntArrayCuckoo(valueSegmentsConfig);
        offsetListStore = new RAMOffsetListStore(nsOptions);
        metaRWLock = new ReentrantReadWriteLock();
        metaReadLock = metaRWLock.readLock();
        metaWriteLock = metaRWLock.writeLock();
        rwLock = new ReentrantReadWriteLock();
        readLock = rwLock.readLock();
        writeLock = rwLock.writeLock();
        pendingWaitFors = new ConcurrentSkipListMap<>(DHTKeyComparator.dhtKeyComparator);
        minVersion = nsProperties.getMinVersion();
        if (minVersion > Long.MIN_VALUE) {
            curSnapshot = minVersion - 1;
            // FUTURE - probably eliminate curSnapshot
        } else {
            curSnapshot = Long.MIN_VALUE;
        }
        this.activeRetrievals = activeRetrievals;
        systemTimeSource = SystemTimeUtil.systemTimeSource;
        nsStats = new NamespaceStats();
        deletedSegments = new HashSet<>();
        
    	Pair<PutTrigger,RetrieveTrigger>	triggers;    	
    	triggers = instantiateServerSideCode(nsOptions.getNamespaceServerSideCode());
    	putTrigger = triggers.getV1();
    	retrieveTrigger = triggers.getV2();
    	if (putTrigger != null) {
    		putTrigger.initialize(this);
    	}
    	if (retrieveTrigger != null && retrieveTrigger != putTrigger) {
    		retrieveTrigger.initialize(this);
    	}
    }
    
    private void createInitialHeadSegment() {
        FileSegment.SyncMode    syncMode;
        
        if (nextSegmentID.get() != 0) {
        	throw new RuntimeException("nextSegmentID.get() != 0");
        }
        Log.warning("Creating initial head segment");
        syncMode = FileSegment.SyncMode.NoSync;
        switch (nsOptions.getStorageType()) {
        case FILE_SYNC:
            syncMode = FileSegment.SyncMode.Sync;
            // fall through
        case FILE:
            try {
                headSegment = FileSegment.create(nsDir, nextSegmentID.getAndIncrement(), 
                                                 nsOptions.getSegmentSize(), syncMode, nsOptions);
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
            break;
        case RAM:
            headSegment = RAMSegment.create(nsDir, nextSegmentID.getAndIncrement(), 
                                            nsOptions.getSegmentSize(), nsOptions);
            ramSegments.put(headSegment.getSegmentNumber(), (RAMSegment)headSegment);
            break;
        default:
            throw new RuntimeException("panic");
        }
    }
    
    private static final boolean isNonBlankNonDefaultSSCode(NamespaceServerSideCode	ssCode) {
    	if (ssCode == null) {
    		return false;
    	} else {
    		if (StringUtil.isNullOrEmptyTrimmed(ssCode.getUrl()) && StringUtil.isNullOrEmptyTrimmed(ssCode.getPutTrigger()) && StringUtil.isNullOrEmptyTrimmed(ssCode.getRetrieveTrigger())) {
    			return false;
    		} else {
    			return true;
    		}
    	}
    }
    
    private static final Pair<PutTrigger,RetrieveTrigger> instantiateServerSideCode(NamespaceServerSideCode	ssCode) {
        PutTrigger		putTrigger;
        RetrieveTrigger	retrieveTrigger;
        
		putTrigger = null;
		retrieveTrigger = null;
        if (isNonBlankNonDefaultSSCode(ssCode)) {
	    	if (ssCode.getUrl() != null && ssCode.getUrl().trim().length() != 0) {
	    		Log.warningf("Ignoring server side code %s. Remote code not currently supported", ssCode.getUrl());
	    	} else {
				try {
		    		putTrigger = (PutTrigger)Class.forName(ssCode.getPutTrigger()).newInstance();
		    		if (ssCode.getPutTrigger().equals(ssCode.getRetrieveTrigger())) {
		    			retrieveTrigger = (RetrieveTrigger)putTrigger;
		    		} else {
		        		retrieveTrigger = (RetrieveTrigger)Class.forName(ssCode.getRetrieveTrigger()).newInstance();
		    		}
				} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
					Log.logErrorWarning(e, "Unable to instantiate server side code: "+ ssCode);
				}
	    	}
        }
		return new Pair<>(putTrigger, retrieveTrigger);
    }
    
    public NamespaceStore(long ns, File nsDir, DirCreationMode dirCreationMode, NamespaceProperties nsProperties,
            MessageGroupBase mgBase, NodeRingMaster2 ringMaster, boolean isRecovery,
            ConcurrentMap<UUIDBase, ActiveProxyRetrieval> activeRetrievals) {
        this(ns, nsDir, dirCreationMode, nsProperties, null, mgBase, ringMaster, isRecovery, activeRetrievals);
    }
    
    private void initRAMSegments() {
        headSegment = RAMSegment.create(nsDir, nextSegmentID.getAndIncrement(), 
                nsOptions.getSegmentSize(), nsOptions);
        ramSegments.put(headSegment.getSegmentNumber(), (RAMSegment)headSegment);
    }
    
    public void startWatches(ZooKeeperExtended zk, String nsLinkBasePath, LinkCreationListener linkCreationListener) {        
        if (parent == null && nsOptions.getVersionMode() == NamespaceVersionMode.SINGLE_VERSION 
                           && nsOptions.getAllowLinks()) {
            watchForLink(zk, nsLinkBasePath, linkCreationListener);
        }
    }
    
    private void watchForLink(ZooKeeperExtended zk, String nsLinkBasePath, LinkCreationListener linkCreationListener) {
        new LinkCreationWatcher(zk, nsLinkBasePath, ns, linkCreationListener);
    }
    
    public void linkParent(NamespaceStore parent) {
        if (this.parent == null) {
        	Log.warningf("%x linked to parent %x", ns, parent.getNamespace());
            this.parent = parent;
        } else {
            Log.warning("Ignoring extra call to linkParent");
        }
    }
        
    public long getNamespace() {
        return ns;
    }
    
    public NamespaceProperties getNamespaceProperties() {
    	return nsProperties;
    }
    
    public NamespaceStats getNamespaceStats() {
        return nsStats;
    }
    
    public long getTotalKeys() {
        return nsStats.getTotalKeys();
    }

    public NamespaceOptions getNamespaceOptions() {
        return nsOptions;
    }
    
    protected boolean isDynamic() {
        return false;
    }
    
    public NodeRingMaster2 getRingMaster() {
        return ringMaster;
    }

    // must hold lock
    private void valueSegmentsPut(DHTKey key, int value) {
        try {
            valueSegments.put(key, value);
        } catch (TableFullException tfe) {
            Log.warningAsync(String.format("valueSegments full %x. Creating new table.", ns));
            valueSegments = IntArrayCuckoo.rehashAndAdd(valueSegments, key, value);
        }
    }

    // used by recovery. no lock needed in recovery
    private void setHeadSegment(FileSegment segment) {
        Log.warning("Setting head segment: ", segment.getSegmentNumber());
        this.headSegment = segment;
        nextSegmentID.set(segment.getSegmentNumber() + 1);
    }

    // writeLock must be held
    private void newHeadSegment() {
        WritableSegmentBase oldHead;

        // headCreationLock is currently redundant since we already have a write lock
        // Think about whether or not we want to keep it. Would it ever be needed?
        //headCreationLock.lock();
        try {
            WritableSegmentBase newHead;
            FileSegment.SyncMode    syncMode;

            syncMode = FileSegment.SyncMode.NoSync;
            switch (nsOptions.getStorageType()) {
            case FILE_SYNC:
                syncMode = FileSegment.SyncMode.Sync;
                // fall through
            case FILE:
                newHead = FileSegment.create(nsDir, nextSegmentID.getAndIncrement(), 
                                             nsOptions.getSegmentSize(), syncMode, nsOptions);
                break;
            case RAM:
                RAMSegment  ramSegment;
                
                ramSegment = RAMSegment.create(nsDir, nextSegmentID.getAndIncrement(), 
                                               nsOptions.getSegmentSize(), nsOptions);
                ramSegments.put(ramSegment.getSegmentNumber(), ramSegment);
                newHead = ramSegment;
                break;
            default: throw new RuntimeException();
            }
            oldHead = headSegment;
            headSegment = newHead;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        //} finally {
            //headCreationLock.unlock();
        }

        if (nsOptions.getStorageType().isFileBased()) {
            // FUTURE - persistence may be incomplete...think about this
            try {
                oldHead.persist();
                if (debugSegments) {
                    Log.warning("persisted segment: " + oldHead.getSegmentNumber());
                }
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
            // FUTURE - consider persisting in another thread - would need to handle mutual exclusion, consistency, etc.
        }
    }

    // lock must be held
    private Integer newestSegment(DHTKey key) {
        metaReadLock.lock();
        try {
            Integer newestSegment;
    
            newestSegment = valueSegments.get(key);
            if (newestSegment == IntCuckooConstants.noSuchValue) {
                newestSegment = null; // FIXME - TEMP
            }
            if (newestSegment != null) {
                if (newestSegment >= 0) {
                    return newestSegment;
                } else {
                    OffsetList offsetList;
    
                    offsetList = offsetListStore.getOffsetList(-newestSegment);
                    return offsetList.getLastOffset();
                }
            } else {
                return null;
            }
        } finally {
            metaReadLock.unlock();
        }
    }
    
    // writeLock must be held
    private VersionCheckResult checkPutVersion(DHTKey key, long version) {
        if (version <= curSnapshot || version < minVersion || !nsOptions.getVersionMode().validVersion(version)) {
            return VersionCheckResult.Invalid;
        } else {
            if (nsOptions.getRevisionMode() == RevisionMode.NO_REVISIONS) {
                long segmentNewestVersion;
    
                segmentNewestVersion = newestVersion(key);
                if (segmentNewestVersion >= 0) {
                    if (debugVersion) {
                        System.out.printf("version %d segmentNewestVersion %d\n", version, segmentNewestVersion);
                    }
                    if (version > segmentNewestVersion) {
                        return VersionCheckResult.Valid;
                    } else if (version < segmentNewestVersion) {
                        return VersionCheckResult.Invalid;
                    } else {
                        return VersionCheckResult.Equal;
                    }
                } else {
                    return VersionCheckResult.Valid;
                }
            } else {
                return VersionCheckResult.Valid;
            }
        }
    }
    
    // write lock must be held
    private long newestVersion(DHTKey key) {
        Integer newestSegment;
    
        newestSegment = newestSegment(key);
        if (debugVersion) {
            System.out.printf("newestVersion(%s)\n", key);
            System.out.println("newestSegment: " + newestSegment);
        }
        if (newestSegment != null && newestSegment >= 0) {
            long[] segmentNewestVersionAndStorageTime;
    
            if (newestSegment != headSegment.getSegmentNumber()) {
                segmentNewestVersionAndStorageTime = segmentNewestVersion(newestSegment, key);
            } else {
                segmentNewestVersionAndStorageTime = segmentVersionAndStorageTime(headSegment, key, newestVersionOptions);
            }
            return segmentNewestVersionAndStorageTime[VERSION_INDEX];
        } else {
            return -1;
        }
    }

    // write lock must be held
    private Set<Waiter> checkPendingWaitFors(DHTKey key) {
        if (debugWaitFor) {
            System.out.println("checkPendingWaitFors");
            System.out.printf("pendingWaitFors.size() %d\n", pendingWaitFors.size());
        }
        if (pendingWaitFors.size() > 0) {
            Set<Waiter> triggeredWaiters;
            Collection<PendingWaitFor> pendingWaitForCollection;

            triggeredWaiters = null;
            pendingWaitForCollection = pendingWaitFors.get(key);
            if (pendingWaitForCollection != null) {
                for (PendingWaitFor pendingWaitFor : pendingWaitForCollection) {
                    ByteBuffer result;

                    if (debugWaitFor) {
                    	System.out.printf("pendingWaitFor %s options %s\n", pendingWaitFor, pendingWaitFor.getOptions());
                    }
                    result = _retrieve(key, pendingWaitFor.getOptions());
                    if (result != null) {
                        Waiter waiter;

                        // we have a result, now we need to send it back...
                        waiter = activeRetrievals.get(pendingWaitFor.getOpUUID());
                        if (waiter != null) {
                            if (triggeredWaiters == null) {
                                triggeredWaiters = new HashSet<>();
                            }
                            triggeredWaiters.add(waiter);
                            // duplicate the result so that each
                            // waiter gets its own
                            if (debugWaitFor) {
                                System.out.printf("Triggering waiter for %s\n", pendingWaitFor.getOpUUID());
                            }
                            waiter.waitForTriggered(key, result.duplicate());
                            // Give the waiter the result of the waitfor.
                            // In addition, we pass back the triggered wait fors so that
                            // the waiter can send messages back when
                            // they have all been gathered.
                        } else {
                            if (debugWaitFor) {
                                System.out.printf("No waiter found for %s\n", pendingWaitFor.getOpUUID());
                            }
                        }
                        pendingWaitForCollection.remove(pendingWaitFor);
                    } else {
                        if (debugWaitFor) {
                            System.out.printf("No result found for %s\n", KeyUtil.keyToString(key));
                        }
                    }
                }
            } else {
                if (debugWaitFor) {
                    System.out.println("pendingWaitForCollection not found");
                }
            }
            return triggeredWaiters;
        } else {
            return null;
        }
    }

    private void handleTriggeredWaitFors(Set<Waiter> triggeredWaitFors) {
        if (debugWaitFor) {
            System.out.println("handleTriggeredWaitFors");
        }
        for (Waiter triggeredWaitFor : triggeredWaitFors) {
            triggeredWaitFor.relayWaitForResults();
        }
    }
    
    public void put(List<StorageValueAndParameters> values, byte[] userData, KeyedOpResultListener resultListener) {
        Set<Waiter> triggeredWaitFors;
        NamespaceVersionMode    nsVersionMode;
        boolean	locked;

        triggeredWaitFors = null;
        
        nsVersionMode = nsOptions.getVersionMode();
        
        //LWTThreadUtil.setBlocked();
        writeLock.lock();
        try {
            // System.out.printf("NamespaceStore.put() group size: %d\n", values.size());
            for (StorageValueAndParameters value : values) {
                OpResult storageResult;

                if (putTrigger != null) {
                	storageResult = putTrigger.put(this, value.getKey(), value.getValue(), new SSStorageParametersImpl(value, value.getValue().remaining()), userData, nsVersionMode);
                } else {
                	storageResult = _put(value.getKey(), value.getValue(), value, userData, nsVersionMode);
                }
                //if (storageResult != OpResult.SUCCEEDED) Log.warningf("fail _put %s %s %d", KeyUtil.keyToString(value.getKey()), storageResult, value.getVersion()); // for debugging
                resultListener.sendResult(value.getKey(), storageResult);
                if (storageResult == OpResult.SUCCEEDED) {
                    Set<Waiter> _triggeredWaitFors;

                    _triggeredWaitFors = checkPendingWaitFors(value.getKey());
                    if (_triggeredWaitFors != null) {
                        if (triggeredWaitFors == null) {
                            triggeredWaitFors = new HashSet<>();
                        }
                        triggeredWaitFors.addAll(_triggeredWaitFors);
                    }
                }
            }
        } finally {
            writeLock.unlock();
            //LWTThreadUtil.setNonBlocked();
        }
        if (triggeredWaitFors != null) {
            handleTriggeredWaitFors(triggeredWaitFors);
        }
    }
    
    /*
     * 
     * The PendingPut implementation is currently commented out. It's utility is yet to be determined. A more pressing optimization is to 
     * allow a per-thread NamespaceStore so that RAM stores may be lockless.
    
    private static class PendingPut {
    	final List<StorageValueAndParameters> values;
    	final byte[] userData;
    	final KeyedOpResultListener resultListener;
    	
    	PendingPut(List<StorageValueAndParameters> values, byte[] userData, KeyedOpResultListener resultListener) {
    		this.values = values;
    		this.userData = userData;
    		this.resultListener = resultListener;
    	}
    }
    
    private final LightLinkedBlockingQueue<PendingPut>	pendingPuts = new LightLinkedBlockingQueue<>();
    private volatile	boolean	lockedWillPoll;
    
    public void put(List<StorageValueAndParameters> _values, byte[] _userData, KeyedOpResultListener _resultListener) {
        boolean	locked;
        PendingPut	storedPendingPut;

        storedPendingPut = null;
        //LWTThreadUtil.setBlocked();
        locked = writeLock.tryLock();
        if (!locked) {
        	try {
        		storedPendingPut = new PendingPut(_values, _userData, _resultListener);
				pendingPuts.put(storedPendingPut);
        		if (!lockedWillPoll) {
        			writeLock.lock();
        			locked = true;
        		}
			} catch (InterruptedException e) {
				throw new RuntimeException("Panic");
			}
        }
        if (locked) {
	        try {
	        	List<StorageValueAndParameters> values;
	        	byte[] userData;
	        	KeyedOpResultListener resultListener;

	        	lockedWillPoll = true;
	        	if (storedPendingPut != null) {
		        	boolean	prevPendingPutFound;
		        	
	        		prevPendingPutFound = pendingPuts.remove(storedPendingPut);
	        		if (!prevPendingPutFound) {
			            PendingPut	pendingPut;
			            
	    	        	lockedWillPoll = false;
		        		pendingPut = pendingPuts.poll();
			            if (pendingPut == null) {
		        			return;
			            } else {
				        	_values = pendingPut.values;
				        	_userData = pendingPut.userData;
				        	_resultListener = pendingPut.resultListener;
			            }
	        		}
	        	}
	        	values = _values;
	        	userData = _userData;
	        	resultListener = _resultListener;
	        	do {
		            PendingPut	pendingPut;
		            
	        		lockedWillPoll = true;
	        		__put(values, userData, resultListener);

	        		lockedWillPoll = false;
	        		pendingPut = pendingPuts.poll();
		            if (pendingPut == null) {
			            writeLock.unlock();
			            pendingPut = pendingPuts.poll();
			            if (pendingPut == null) {
			            	break;
			            } else {
			            	writeLock.lock();
			            }
		            } else {
			        	values = pendingPut.values;
			        	userData = pendingPut.userData;
			        	resultListener = pendingPut.resultListener;
		            }		            	
	            } while (true);
	        } finally {
	        	if (((ReentrantReadWriteLock)rwLock).isWriteLockedByCurrentThread()) {
	        		writeLock.unlock();
	        	}
	            //LWTThreadUtil.setNonBlocked();
	        }
        }
    }

    // lock must be held
    private void __put(List<StorageValueAndParameters> values, byte[] userData, KeyedOpResultListener resultListener) {
        NamespaceVersionMode    nsVersionMode;
        Set<Waiter> triggeredWaitFors;

        triggeredWaitFors = null;
        nsVersionMode = nsOptions.getVersionMode();
        // System.out.printf("NamespaceStore.put() group size: %d\n", values.size());
        for (StorageValueAndParameters value : values) {
            OpResult storageResult;

            storageResult = _put(value.getKey(), value.getValue(), value, userData, nsVersionMode);
            resultListener.sendResult(value.getKey(), storageResult);
            if (storageResult == OpResult.SUCCEEDED) {
                Set<Waiter> _triggeredWaitFors;

                _triggeredWaitFors = checkPendingWaitFors(value.getKey());
                if (_triggeredWaitFors != null) {
                    if (triggeredWaitFors == null) {
                        triggeredWaitFors = new HashSet<>();
                    }
                    triggeredWaitFors.addAll(_triggeredWaitFors);
                }
            }
        }
        if (triggeredWaitFors != null) {
        	writeLock.unlock();
        	try {
        		handleTriggeredWaitFors(triggeredWaitFors);
        	} finally {
            	writeLock.lock();
        	}
        }
    }
    */
    
    /**
     * Checks to see if this put() should be allowed to proceed on the basis that it is a duplicate of a previous
     * storage operation.
     * 
     * @param key
     * @param value
     * @param storageParams
     * @param userData
     * @return
     */
    private SegmentStorageResult checkForDuplicateStore(DHTKey key, ByteBuffer value, StorageParameters storageParams,
            byte[] userData) {
        RetrievalOptions options;
        ByteBuffer result;
        int	debug = 0;

        // this comparison isn't returning invalid version like it should for some cases
        options = OptionsHelper.newRetrievalOptions(RetrievalType.VALUE, WaitMode.GET, 
                        VersionConstraint.exactMatch(storageParams.getVersion()), 
                        NonExistenceResponse.EXCEPTION, true);
        result = _retrieve(key, options);
        if (result == null) {
        	debug = -1;
            // previous storage operation is not complete
            return SegmentStorageResult.previousStoreIncomplete;
        } else {
            if (!StorageProtocolUtil.storageStateValidForRead(nsOptions.getConsistencyProtocol(), 
                    MetaDataUtil.getStorageState(result, 0))) {
            	debug = -2;
                // previous storage operation is not complete
                return SegmentStorageResult.previousStoreIncomplete;
            } else {
                // FUTURE - allow for no mutation detection on a per-namespace basis?
                if (storageParams.getChecksumType() == MetaDataUtil.getChecksumType(result, 0)) {
                    if (storageParams.getChecksumType() != ChecksumType.NONE) {
                        if (Arrays.equals(MetaDataUtil.getChecksum(result, 0), storageParams.getChecksum())) {
                        	debug = -10;
                            return SegmentStorageResult.duplicateStore;
                        } else {
                        	debug = 1;
                            return SegmentStorageResult.mutation;
                        }
                    } else {
                        if (BufferUtil.equals(result, MetaDataUtil.getDataOffset(result, 0), value, 0, value.limit())) {
                        	debug = -11;
                            return SegmentStorageResult.duplicateStore;
                        } else {
                        	debug = 2;
                            return SegmentStorageResult.mutation;
                        }
                    }
                } else {
                	debug = 3;
                    return SegmentStorageResult.mutation;
                }
            }
        }
    }

    protected final OpResult _put(DHTKey key, ByteBuffer value, StorageParameters storageParams, byte[] userData, 
                                NamespaceVersionMode nsVersionMode) {
        WritableSegmentBase     storageSegment;
        SegmentStorageResult    storageResult;
        VersionCheckResult      versionCheckResult;

        if (testStorageCorruption && !isDynamic() && ns != NamespaceUtil.metaInfoNamespace.contextAsLong()) {
            MetaDataUtil.testCorruption(value, storageCorruptionProbability, value.limit() - 2);
        }
        
        if (debugVersion) {
            System.out.println("StorageParameters: " + storageParams);
        }
        if (debug) {
            System.out.println("_put " + key + " " + value);
            System.out.println(userData == null ? "null" : userData.length);
        }
        
        // FUTURE - Improve below. Note that we've moved time-based out of here
        // for now, and possibly forever. The feature that currently is not supported
        // is sequential versioning
        if (storageParams.getVersion() == DHTConstants.unspecifiedVersion 
                && nsOptions.getVersionMode().isSystemSpecified()) { 
            //versionCheckResult = VersionCheckResult.Valid;
            //storageParams = getSystemVersionParams(key, storageParams);
            throw new RuntimeException("Panic"); // moved to ActiveProxyPut for now
        } else {
            versionCheckResult = checkPutVersion(key, storageParams.getVersion());
        }
        switch (versionCheckResult) {
        case Invalid:
            if (debug) {
                System.out.println("_put returning INVALID_VERSION");
            }
            return OpResult.INVALID_VERSION;
        case Equal:
            storageResult = checkForDuplicateStore(key, value, storageParams, userData);
            if (debug) {
                System.out.printf("checkForDuplicateStore result %s\n", storageResult);
            }
            if (storageResult != SegmentStorageResult.previousStoreIncomplete) {
                break;
            } else {
                // fall through and store fresh
            }
            if (debug) {
                System.out.printf("fall through after checkForDuplicateStore\n");
            }
        case Valid:
            storageSegment = headSegment;
            try {
                storageResult = storageSegment.put(key, value, storageParams, userData, nsOptions);
            } catch (TableFullException tfe) {
                storageResult = SegmentStorageResult.segmentFull;
                // FUTURE - think about resizing the segment table instead
                // of flipping to a new segment
            }
            while (storageResult.callerShouldRetry()) {
                if (value.remaining() > headSegment.getDataSegmentSize()) {
                    throw new RuntimeException("value > segment size");
                    // FIXME - we don't support values > segment size
                    // client needs to enforce this
                }
                newHeadSegment();
                storageSegment = headSegment;
                storageResult = storageSegment.put(key, value, storageParams, userData, nsOptions);
            }
            if (storageResult == SegmentStorageResult.stored) {
                putSegmentNumberAndVersion(key, storageSegment.getSegmentNumber(), storageParams.getVersion(), 
                                           storageParams.getCreationTime());
                addToSizeStats(storageParams);
            } else {
                if (debugSegments) {
                    Log.warning("not stored "+ storageResult);
                }
            }
            break;
        default:
            throw new RuntimeException("panic");
        }
        //if (storageResult != SegmentStorageResult.stored) Log.warningf("!stored _put2 %s %s %d %s", KeyUtil.keyToString(key), storageResult, storageParams.getVersion(), storageParams); // for debugging
        return storageResult.toOpResult();
    }
    
    public void addToSizeStats(StorageParameters storageParams) {
        addToSizeStats(storageParams.getUncompressedSize(), storageParams.getCompressedSize());
    }

    public void addToSizeStats(int uncompressedSize, int compressedSize) {
        nsStats.addBytes(uncompressedSize, compressedSize);
    }
    
    private StorageParameters getSystemVersionParams(DHTKey key, StorageParameters storageParams) {
        long    version;
        
        switch (nsOptions.getVersionMode()) {
        case SEQUENTIAL: version = newestVersion(key) + 1; break;
        case SYSTEM_TIME_MILLIS: version = systemTimeSource.absTimeMillis(); break;
        case SYSTEM_TIME_NANOS: version = systemTimeSource.absTimeNanos(); break;
        default: throw new RuntimeException("non-system or unexpected VersionMode");
        }
        return storageParams.version(version);
    }
    
    public void putSegmentNumberAndVersion(DHTKey key, int segmentNumber, long version, long creationTime) {
        metaWriteLock.lock();
        try {
            int	rawPrevSegment;
    
            if (debugSegments) {
                Log.warning("putSegmentNumberAndVersion " + key + " " + segmentNumber + " " + version);
            }
            rawPrevSegment = valueSegments.get(key);
            if (debugSegments) {
                Log.warning("prevSegment: ", rawPrevSegment);
            }
            if (rawPrevSegment == IntCuckooConstants.noSuchValue) {
            	if (nsOptions.isWriteOnce() || nsOptions.getStorageType() == StorageType.RAM) {
	                valueSegmentsPut(key, segmentNumber);
            	} else {
                    OffsetList	offsetList;

                    // the current pkc doesn't support version storage; we must either use an offset list,
                    // or touch disk to get the version
                    // (touching disk is expensive when computing checksum trees)
                    // Similar logic in WritableSegmentBase._put()
                    if (debugSegments) {
                        Log.warning("multi versioning using offset list");
                    }
                    offsetList = offsetListStore.newOffsetList();
                    offsetList.putOffset(version, segmentNumber, creationTime);
                    valueSegmentsPut(key, -((RAMOffsetList)offsetList).getIndex());
            	}
                // new key, record it in stats
                nsStats.incTotalKeys();
            } else if (rawPrevSegment >= 0) {
            	int	prevSegment;
            	
            	prevSegment = rawPrevSegment;
                // this key has been stored previously; check to see if
                // we need to do more work
                if (false && prevSegment == segmentNumber) {
                    // stored in the same segment; no action necessary
                    if (debugSegments) {
                        Log.warning("prevSegment == storageSegment.getSegmentNumber()");
                    }
                } else {
    	            // store in a different segment; ensure multiple
    	            // segments are set up correctly
                    OffsetList offsetList;
                    long[] prevVersionAndStorageTime;
                    boolean removed;

                    // only a single previous segment exists
                    // we need to save the old and the new segments in the
                    // offset list
                    if (debugSegments) {
                        Log.warning("prevSegment >= 0");
                    }
                    offsetList = offsetListStore.newOffsetList();
                    prevVersionAndStorageTime = segmentOldestVersion(prevSegment, key);
                    assert prevVersionAndStorageTime[VERSION_INDEX] != noSuchVersion;
                    if (debugSegments) {
                        Log.warning("prevVersion: ", prevVersionAndStorageTime[VERSION_INDEX]);
                    }
                    offsetList.putOffset(prevVersionAndStorageTime[VERSION_INDEX], prevSegment, 
                                         prevVersionAndStorageTime[STORAGE_TIME_INDEX]);
                    offsetList.putOffset(version, segmentNumber, creationTime);
                    removed = valueSegments.remove(key);
                    if (!removed) {
                        System.out.println("\n\n");
                        Log.warning(valueSegments.get(key));
                        throw new RuntimeException("valueSegments.remove() failed for: " + key);
                    }
                    if (debugSegments) {
                        Log.warning("removed valueSegments: ", key);
                        if (valueSegments.get(key) != IntCuckooConstants.noSuchValue) {
                            Log.warning(valueSegments.get(key));
                            throw new RuntimeException("remove failed");
                        }
                    }
                    valueSegmentsPut(key, -((RAMOffsetList)offsetList).getIndex());
                }
            } else {
                OffsetList	offsetList;
                int			prevSegment;

                if (debugSegments) {
                    Log.warning("prevSegment < 0");
                }
                offsetList = offsetListStore.getOffsetList(-rawPrevSegment);
                prevSegment = offsetList.getLastOffset();
                if (false && prevSegment == segmentNumber) {
                    // stored in the same segment; no action necessary
                    if (debugSegments) {
                        Log.warning("prevSegment == storageSegment.getSegmentNumber()");
                    }
                } else {
                	offsetList.putOffset(version, segmentNumber, creationTime);
                }
            }
        } finally {
            metaWriteLock.unlock();
        }
    }    

    public List<OpResult> putUpdate(List<? extends DHTKey> updates, long version) {
        List<OpResult>  results;
        Set<Waiter>     triggeredWaitFors;
        
        triggeredWaitFors = null;
        results = new ArrayList<>(updates.size());
        writeLock.lock();
        try {
            for (DHTKey update : updates) {
                MessageGroupKeyOrdinalEntry entry;
                OpResult    result;
                
                entry = (MessageGroupKeyOrdinalEntry)update;
                result = _putUpdate(entry, version, entry.getOrdinal());
                results.add(result);
                if (result == OpResult.SUCCEEDED 
                        && StorageProtocolUtil.storageStateValidForRead(
                                nsOptions.getConsistencyProtocol(), entry.getOrdinal())) {
                    Set<Waiter> _triggeredWaitFors;

                    _triggeredWaitFors = checkPendingWaitFors(update);
                    if (_triggeredWaitFors != null) {
                        if (triggeredWaitFors == null) {
                            triggeredWaitFors = new HashSet<>();
                        }
                        triggeredWaitFors.addAll(_triggeredWaitFors);
                    }
                }
            }
        } finally {
            writeLock.unlock();
        }
        if (triggeredWaitFors != null) {
            handleTriggeredWaitFors(triggeredWaitFors);
        }
        return results;
    }
    
    public OpResult putUpdate_(DHTKey key, long version, byte storageState) {
        OpResult    result;
        Set<Waiter> triggeredWaitFors;
        
        if (debug) {
            System.out.println("Single key putUpdate()");
        }
        triggeredWaitFors = null;
        writeLock.lock();
        try {
            result = _putUpdate(key, version, storageState);
            //if (result != OpResult.SUCCEEDED) Log.warningf("fail putUpdate %s %s %d", KeyUtil.keyToString(key), result, version); // for debugging
            if (result == OpResult.SUCCEEDED 
                    && StorageProtocolUtil.storageStateValidForRead(
                            nsOptions.getConsistencyProtocol(), storageState)) {
                triggeredWaitFors = checkPendingWaitFors(key);
            }
        } finally {
            writeLock.unlock();
        }
        if (triggeredWaitFors != null) {
            handleTriggeredWaitFors(triggeredWaitFors);
        }
        return result;
    }
    
    private OpResult _putUpdate(DHTKey key, long version, byte storageState) {
    	if (putTrigger != null) {
    		return putTrigger.putUpdate(this, key, version, storageState);
    	} else {
    		return __putUpdate(key, version, storageState);
    	}
    }
    
    @Override
    public OpResult putUpdate(DHTKey key, long version, byte storageState) {
    	return __putUpdate(key, version, storageState);
    }
    
    private OpResult __putUpdate(DHTKey key, long version, byte storageState) {
        OpResult result;
        int segmentNumber;

        if (debugVersion) {
            System.out.println("putUpdate:\t" + key);
            System.out.println("version:\t" + version);
        }
        segmentNumber = getSegmentNumber(key, VersionConstraint.exactMatch(version));
        assert segmentNumber >= 0 || segmentNumber == IntCuckooConstants.noSuchValue;
        if (segmentNumber == IntCuckooConstants.noSuchValue) {
            if (debug) {
                System.out.println("_putUpdate returning INVALID_VERSION");
            }
            Log.warningf("Couldn't find %s %d in _putUpdate()", KeyUtil.keyToString(key), version);
            return OpResult.ERROR;
            //return OpResult.INVALID_VERSION;
        } else {
            if (headSegment.getSegmentNumber() == segmentNumber) {
                if (debugSegments) {
                    Log.warning("PutUpdate, head segment");
                }
                result = headSegment.putUpdate(key, version, storageState);
                if (debugSegments) {
                    Log.warning("Done PutUpdate, head segment "+ result +" "+ storageState);
                }
            } else {
                try {
                    WritableSegmentBase segment;
                	/*
                    FileSegment.SyncMode    syncMode;

                    syncMode = FileSegment.SyncMode.NoSync;
                    switch (nsOptions.getStorageType()) {
                    case RAM: 
                        segment = ramSegments.get(segmentNumber);
                        break;
                    case FILE_SYNC:
                        syncMode = FileSegment.SyncMode.Sync;
                        // fall through
                    case FILE: 
                        // FUTURE - consider getting this from file segment cache
                        segment = FileSegment.openForDataUpdate(nsDir, segmentNumber, 
                                                                nsOptions.getSegmentSize(), syncMode, nsOptions);
                        break;
                    default: throw new RuntimeException("Panic");
                    }
                    */
                    
                    segment = getFileSegment(segmentNumber, SegmentPrereadMode.NoPreread);
                    try {
                        if (debugSegments) {
                            Log.warning("Read from file segment");
                        }
                        result = segment.putUpdate(key, version, storageState);
                        if (debugSegments) {
                            Log.warning("Done read from file segment");
                            Log.warning("result: " + result);
                        }
                    } finally {
                        switch (nsOptions.getStorageType()) {
                        case RAM:
                            break;
                        case FILE: 
                            //((FileSegment)segment).close();
                            ((FileSegment)segment).removeReference();
                            break;
                        default: throw new RuntimeException("Panic");
                        }
                    }
                } catch (IOException ioe) {
                    Log.logErrorWarning(ioe);
                    return OpResult.ERROR;
                }
            }
            return result;
        }
    }
    
    // For analysis only
    private static final AtomicInteger  commonSegment = new AtomicInteger();
    private static final AtomicInteger  totalKeys = new AtomicInteger();
    
    public List<ByteBuffer> retrieve(List<? extends DHTKey> keys, InternalRetrievalOptions options, UUIDBase opUUID) {
    	if (retrieveTrigger == null) {
	        DHTKey[]  		_keys;
	        ByteBuffer[]	_results;
	        
	        if (debugVersion) {
	            System.out.printf("retrieve internal options: %s\n", options);
	        }        
	        _keys = new DHTKey[keys.size()];
	        for (int i = 0; i < _keys.length; i++) {
	        	_keys[i] = keys.get(i);
	        }
	        
	        if (_keys.length > 1) {
	        	_results = _retrieve(_keys, options);
	        } else {
	        	// special case single retrieval
	        	_results = new ByteBuffer[1];
	        	_results[0] = _retrieve(_keys[0], options);
	        }
	        
	        for (int i = 0; i < _results.length; i++) {
	            if (parent != null) {
	                VersionConstraint   vc;
	                
	                if (debugParent) {
	                    Log.warning("parent != null");
	                }
	                vc = options.getVersionConstraint();
	                
	                // We look in parent if the vc could possibly be answered by the parent
	                // in a way that would override what we have from this namespace.
	                
	                if (_results[i] == null) {
	                    if (debugParent) {
	                        Log.warningf("%x null result. Checking parent %x.", ns, parent.getNamespace());
	                    }
	                    // If result from this ns is null, look in the parent.
	                    _results[i] = parent._retrieve(_keys[i], makeOptionsForNestedRetrieve(options));
	                    if (debugParent) {
	                        if (_results[i] != null) {
	                            Log.warning("Found result in parent");
	                        }
	                    }
	                } else {
	                    // If we have a non-null value from this ns, and the vc mode is GREATEST
	                    // then the value that we already have is the best.
	                    // Otherwise for the LEAST case, look in the parent to see if it has a better result.
	                    if (vc.getMode() == VersionConstraint.Mode.LEAST) {
	                        ByteBuffer parentResult;
	
	                        if (debugParent) {
	                            Log.warning("Non-null result, but mode LEAST. checking parent");
	                        }
	                        parentResult = parent._retrieve(_keys[i], makeOptionsForNestedRetrieve(options));
	                        if (parentResult != null) {
	                            // if the parent had any valid result, then - by virtue of the fact
	                            // that all parent versions are < child versions - the parent
	                            // result is preferred
	                        	_results[i] = parentResult;
	                            if (_results[i] != null) {
	                                Log.warning("Found result in parent");
	                            }
	                        }
	                    }
	                }
	            }
	            
	            if (_results[i] == null && options.getWaitMode() == WaitMode.WAIT_FOR
	                    && options.getVersionConstraint().getMax() > curSnapshot) {
	                // Note that since we hold the readLock, a write cannot come
	                // in while we add the pending wait for.
	                addPendingWaitFor(_keys[i], options.getRetrievalOptions(), opUUID);
	            }
	            if (options.getVerifyIntegrity()) {
	            	_results[i] = verifyIntegrity(_keys[i], _results[i]);
	            }
	        }
	        return SKImmutableList.copyOf(_results);
    	} else {
    		return retrieve_nongroupedImpl(keys, options, opUUID);
    	}
    }
    
    public List<ByteBuffer> retrieve_nongroupedImpl(List<? extends DHTKey> keys, InternalRetrievalOptions options, UUIDBase opUUID) {
        List<ByteBuffer> results;
        KeyAndInteger[]  _keys;
        
        /*
        // We sort to attempt to group segment access
        _keys = new KeyAndInteger[keys.size()];
        for (int i = 0; i < _keys.length; i++) {
            DHTKey  k;
            
            k = keys.get(i);
            _keys[i] = new KeyAndInteger(k, getSegmentNumber(k, options.getVersionConstraint()));
        }
        Arrays.sort(_keys, KeyAndInteger.getIntegerComparator());
        
        int prevSegment;
        int groupStart;
        List<KeyAndInteger[]>   keyGroups;
        
        keyGroups = new ArrayList<>();
        groupStart = 0;
        prevSegment = _keys[0].getInteger();
        for (int i = 1; i < _keys.length; i++) {
            int curSegment;
            
            curSegment = _keys[i].getInteger();
            if (curSegment == prevSegment) {
                //commonSegment.incrementAndGet();
            } else {
                KeyAndInteger[] keyGroup;
                
                keyGroup = new KeyAndInteger[i - groupStart];
                System.arraycopy(_keys, groupStart, keyGroup, 0, keyGroup.length);
                keyGroups.add(keyGroup);
                groupStart = i;
            }
            prevSegment = curSegment;
        }
        //totalKeys.addAndGet(_keys.length);
        if (groupStart < _keys.length) {
            KeyAndInteger[] keyGroup;
            
            keyGroup = new KeyAndInteger[_keys.length - groupStart];
            System.arraycopy(_keys, groupStart, keyGroup, 0, keyGroup.length);
            keyGroups.add(keyGroup);
        }
        // temporary for analysis
        //System.out.printf("%d %d\n", commonSegment.get(), totalKeys.get());
        */
        
        if (debugVersion) {
            System.out.printf("retrieve internal options: %s\n", options);
        }
        results = new ArrayList<>(keys.size());
        //readLock.lock();
        //try {
            /*
            for (int i = 0; i < keyGroups.size(); i++) {
                KeyAndInteger[] keyGroup;
                ByteBuffer[]    bufferGroup;

                keyGroup = keyGroups.get(i);
                bufferGroup = _retrieve(keyGroup, options);
                for (int j = 0; j < keyGroup.length; j++) {
                    KeyAndInteger   key;
                    
                    key = keyGroup[j];
            */
            
            for (DHTKey key : keys) {
            //for (KeyAndInteger key : _keys) {
                ByteBuffer result;

                //result = _retrieve(key, options, key.getInteger()/*the segment number*/);
                /*
                if (bufferGroup != null) {
                    result = bufferGroup[j];
                } else {
                    result = null;
                }
                */
                if (retrieveTrigger != null) {
                	result = retrieveTrigger.retrieve(this, key, options);
                } else {
                	result = _retrieve(key, options);
                }
                
                if (parent != null) {
                    VersionConstraint   vc;
                    
                    if (debugParent) {
                        Log.warning("parent != null");
                    }
                    vc = options.getVersionConstraint();
                    
                    // We look in parent if the vc could possibly be answered by the parent
                    // in a way that would override what we have from this namespace.
                    
                    if (result == null) {
                        if (debugParent) {
                            Log.warningf("%x null result. Checking parent %x.", ns, parent.getNamespace());
                        }
                        // If result from this ns is null, look in the parent.
                        result = parent._retrieve(key, makeOptionsForNestedRetrieve(options));
                        if (debugParent) {
                            if (result != null) {
                                Log.warning("Found result in parent");
                            }
                        }
                    } else {
                        // If we have a non-null value from this ns, and the vc mode is GREATEST
                        // then the value that we already have is the best.
                        // Otherwise for the LEAST case, look in the parent to see if it has a better result.
                        if (vc.getMode() == VersionConstraint.Mode.LEAST) {
                            ByteBuffer parentResult;

                            if (debugParent) {
                                Log.warning("Non-null result, but mode LEAST. checking parent");
                            }
                            parentResult = parent._retrieve(key, makeOptionsForNestedRetrieve(options));
                            if (parentResult != null) {
                                // if the parent had any valid result, then - by virtue of the fact
                                // that all parent versions are < child versions - the parent
                                // result is preferred
                                result = parentResult;
                                if (result != null) {
                                    Log.warning("Found result in parent");
                                }
                            }
                        }
                    }
                }
                
                if (result == null && options.getWaitMode() == WaitMode.WAIT_FOR
                        && options.getVersionConstraint().getMax() > curSnapshot) {
                    // Note that since we hold the readLock, a write cannot come
                    // in while we add the pending wait for.
                    addPendingWaitFor(key, options.getRetrievalOptions(), opUUID);
                }
                if (options.getVerifyIntegrity()) {
                    result = verifyIntegrity(key, result);
                }
                results.add(result);
            }
            //}
        //} finally {
        //    readLock.unlock();
        //}
        return results;
    }

    /**
     * Adjust options for retrieval from parent. I.e. consider minVersion or creationTime as
     * appropriate. Note that there is some looseness to the semantics for versioning
     * that relies on clock synchronization.
     * @param options
     * @return
     */
    private InternalRetrievalOptions makeOptionsForNestedRetrieve(InternalRetrievalOptions options) {
        VersionConstraint   oldVC;
        
        // When looking in parents, we only want to see what's present. We don't want 
        // to post any pending wait fors as it is impossible - by design - for a child namespace
        // to observe values stored in parents after child creation. Children are clones,
        // and parents/children are distinct after the creation.
        options = options.waitMode(WaitMode.GET);
        
        oldVC = options.getVersionConstraint();
        switch (nsOptions.getVersionMode()) {
        case SEQUENTIAL:
            throw new RuntimeException("Panic: parent not supported/expected for version mode of SEQUENTIAL");
        case SINGLE_VERSION:
            // for write-once namespaces, we use the system time to limit retrieval in the parent
            // to values that were stored before the child creation
        	if (debugParent) {
        		Log.warning(oldVC);
        		Log.warning(options.versionConstraint(oldVC.maxCreationTime(
                        Math.min(oldVC.getMaxCreationTime(), nsProperties.getCreationTime()))).getVersionConstraint());
        	}
            return options.versionConstraint(oldVC.maxCreationTime(
                    Math.min(oldVC.getMaxCreationTime(), nsProperties.getCreationTime()))); 
        case CLIENT_SPECIFIED:
        case SYSTEM_TIME_MILLIS:
        case SYSTEM_TIME_NANOS:
            // for cases of system-specified versions, we want to ignore values in the parent
            // that were stored after the child namespace was created
            return options.versionConstraint(oldVC.max(
                                                Math.min(oldVC.getMax(), nsProperties.getMinVersion() - 1))); 
        default: throw new RuntimeException("Panic");
        }
    }

    private ByteBuffer verifyIntegrity(DHTKey key, ByteBuffer result) {
        try {
            ValueUtil.verifyChecksum(result);
            return result;
        } catch (CorruptValueException cve) {
            Log.warningAsync(String.format("Corrupt: %s", key));
            return ValueUtil.corruptValue;
        }
    }

    private void addPendingWaitFor(DHTKey key, RetrievalOptions options, UUIDBase opUUID) {
        Set<PendingWaitFor> pendingWaitForSet;

        if (debugWaitFor) {
            System.out.printf("addPendingWaitFor %s %s %s\n", key, options, opUUID);
        }
        pendingWaitForSet = pendingWaitFors.get(key);
        if (pendingWaitForSet == null) {
            Set<PendingWaitFor> prev;

            pendingWaitForSet = new ConcurrentSkipListSet<>();
            prev = pendingWaitFors.putIfAbsent(key, pendingWaitForSet);
            if (prev != null) {
                pendingWaitForSet = prev;
            }
        }
        pendingWaitForSet.add(new PendingWaitFor(key, options, opUUID));
    }

    protected ByteBuffer _retrieve(DHTKey key, RetrievalOptions options) {
        return _retrieve(key, new InternalRetrievalOptions(options));
    }
    
    protected ByteBuffer[] _retrieve(DHTKey[] keys, InternalRetrievalOptions options) {
        int[] segmentNumbers;
        Triple<DHTKey,Integer,Integer>[]	keysSegmentNumbersAndIndices;
        ByteBuffer[]	sortedResults;
        ByteBuffer[]	unsortedResults;
        
        segmentNumbers = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
	        segmentNumbers[i] = getSegmentNumber(keys[i], options.getVersionConstraint());
	        if (debugParent) {
	        	Log.warningf("_retrieve %x %s segmentNumber %d", ns, keys[i], segmentNumbers[i]);
	        }
        }
        
        // Sort so that we group segment access. Not always useful,
        // but can become critical for large retrievals that span multiple segments
        keysSegmentNumbersAndIndices = new Triple[keys.length];
        for (int i = 0; i < keysSegmentNumbersAndIndices.length; i++) {
        	keysSegmentNumbersAndIndices[i] = new Triple<>(keys[i], segmentNumbers[i], i);
        }
        Arrays.sort(keysSegmentNumbersAndIndices, KeyAndSegmentComparator.mostRecentSegmentsFirst);
        
        sortedResults = _retrieve(keysSegmentNumbersAndIndices, options);
        
        // We fix up the ordering here. It would be possible to avoid this
        // by constructing the unsorted results inside of the above _retrieve() call.
        // We currently avoid that to avoid leaking the notion of sorting/indexing changes
        // into that method.
        unsortedResults = new ByteBuffer[sortedResults.length];
        for (int i = 0; i < sortedResults.length; i++) {
        	unsortedResults[keysSegmentNumbersAndIndices[i].getV3()] = sortedResults[i];
        }
        return unsortedResults;
    }
    
    /**
     * Sort by segment number. Break ties using original index.
     */
    private static class KeyAndSegmentComparator implements Comparator<Triple<DHTKey,Integer,Integer>> {
    	private final int	order;
    	
    	static final KeyAndSegmentComparator	mostRecentSegmentsFirst = new KeyAndSegmentComparator(-1);
    	static final KeyAndSegmentComparator	leastRecentSegmentsFirst = new KeyAndSegmentComparator(1);
    	
    	private KeyAndSegmentComparator(int order) {
    		this.order = order;
    	}
    	
		@Override
		public int compare(Triple<DHTKey, Integer, Integer> o1, Triple<DHTKey, Integer, Integer> o2) {
			int	c;
			
			c = order * Integer.compare(o1.getV2(), o2.getV2());
			return c == 0 ? 0 : Integer.compare(o1.getV3(), o2.getV3()); 
		}
    }
    
    /**
     * Called when we find a value in the middle of a write operation. We go back to the
     * previously good stored value. Note that we may need to go back more than one value.
     * @param key
     * @param options
     * @return
     */
    /*
     * FUTURE - consider removal
    protected ByteBuffer _retrievePrevious(DHTKey key, InternalRetrievalOptions options) {
    	InternalRetrievalOptions curPrevOptions;
    	boolean	found;
    	
    	switch (options.getRetrievalType()) {
    	case VALUE:
    		curPrevOptions = options.retrievalType(RetrievalType.VALUE_AND_META_DATA);
    		break;
    	case VALUE_AND_META_DATA:
    	case META_DATA:
    		curPrevOptions = options;
    		break;
    	case EXISTENCE:
    		curPrevOptions = options.retrievalType(RetrievalType.META_DATA);
    		break;
    	default: throw new RuntimeException("Panic");
    	}
    	//options = options.clipAt();
    	while (true) {
    		ByteBuffer	result;
    		
    		result = _retrieve(key, options);
    		if (result == valueStorageStateInvalidForRead) {
    			
    		} else {
    			return result;
    		}
    	}
    }
    */
    
    /*
    protected ByteBuffer[] _retrieve(KeyAndInteger[] keyGroup, InternalRetrievalOptions options) {
        ByteBuffer[]    result;
        int             segmentNumber;

        segmentNumber = keyGroup[0].getInteger(); // all of this group has the same segmentNumber
        if (debugVersion) {
            System.out.println("retrieve:\t" + keyGroup[0]);
            System.out.println("RetrievalOptions:\t" + options);
        }
        if (segmentNumber == IntCuckooConstants.noSuchValue) {
            return null;
        } else {
            if (headSegment.getSegmentNumber() == segmentNumber) {
                // return getValueEntry(key).retrieve(options);
                if (debugSegments) {
                    Log.warning("Read from head segment");
                }
                result = headSegment.retrieve(keyGroup, options);
                if (debugSegments) {
                    Log.warning("Done read from head segment");
                }
            } else {
                try {
                    AbstractSegment segment;

                    segment = getSegment(segmentNumber);
                    try {
                        if (debugSegments) {
                            Log.warning("Read from file segment");
                        }
                        result = segment.retrieve(keyGroup, options);
                        if (debugSegments) {
                            Log.warning("Done read from file segment");
                            Log.warning("result: " + result);
                        }
                    } finally {
                        if (nsOptions.getStorageType() == StorageType.FILE) {
                            ((FileSegment)segment).removeReference();
                        }
                    }
                } catch (IOException ioe) {
                    Log.logErrorWarning(ioe);
                    return null;
                }
            }
            return result;
        }
    }
    */
    
    protected ByteBuffer _retrieve(DHTKey key, InternalRetrievalOptions options) {
        int segmentNumber;
    	VersionConstraint	versionConstraint;
    	int	failedStore;
        
        if (debugParent) {
        	Log.warningf("_retrieve %x %s", ns, key);
        }
    	
        failedStore = 0;
        versionConstraint = options.getVersionConstraint();
        do {
	        ByteBuffer result;
	
	        segmentNumber = getSegmentNumber(key, versionConstraint);
	        if (debugVersion) {
	            System.out.println("retrieve:\t" + key);
	            System.out.println("RetrievalOptions:\t" + options);
	        }
	        if (segmentNumber == IntCuckooConstants.noSuchValue) {
	            return null;
	        } else {
	            readLock.lock();
	            try {
		            if (headSegment.getSegmentNumber() == segmentNumber) {
	                    // return getValueEntry(key).retrieve(options);
	                    if (debugSegments) {
	                        Log.warning("Read from head segment");
	                    }
	                    result = retrieve(headSegment, key, options);
	                    if (debugSegments) {
	                        Log.warning("Done read from head segment");
	                    }
		            } else {
		                try {
		                    AbstractSegment segment;
		
		                    segment = getSegment(segmentNumber, readSegmentPrereadMode);
		                    try {
		                        if (debugSegments) {
		                            Log.warning("Read from file segment");
		                        }
		                        result = retrieve(segment, key, options);
		                        if (debugSegments) {
		                            Log.warning("Done read from file segment");
		                            Log.warning("result: " + result);
		                        }
		                    } finally {
		                        if (nsOptions.getStorageType() == StorageType.FILE) {
		                        	if (segment != headSegment) {
		                        		((FileSegment)segment).removeReference();
		                        	}
		                        }
		                    }
		                } catch (IOException ioe) {
		                    Log.logErrorWarning(ioe);
		                    return null;
		                }
		            }
	            } finally {
	                readLock.unlock();
	            }
	            
	            if (result != null) {
	                // Double check that the result is valid
	                if (debug) {
	                    System.out.printf("tpc %d\n", CCSSUtil.getStorageState(MetaDataUtil.getCCSS(result, 0)));
	                    System.out.printf("tpc %s\n", StorageProtocolUtil.storageStateValidForRead(nsOptions.getConsistencyProtocol(), 
	                            CCSSUtil.getStorageState(MetaDataUtil.getCCSS(result, 0))));
	                }
	                // FUTURE - this is a temporary workaround until the versioned storage is overhauled
	                if (!StorageProtocolUtil.storageStateValidForRead(nsOptions.getConsistencyProtocol(), 
	                                               MetaDataUtil.getStorageState(result, 0))) {
	                    //System.out.printf("key %s storage state: %d\n", KeyUtil.keyToString(key), 
	                    //        CCSSUtil.getStorageState(MetaDataUtil.getCCSS(result, 0)));
	                	switch (versionConstraint.getMode()) {
	                	case GREATEST:
		                	long	newMaxVersion;
		                	
		                    newMaxVersion = MetaDataUtil.getVersion(result, 0) - 1;
		                    if (newMaxVersion < versionConstraint.getMin()) {
		                    	return null;
		                    }
		                    versionConstraint = versionConstraint.max(newMaxVersion);
		                    break;
	                	case LEAST:
		                	long	newMinVersion;
		                	
		                	newMinVersion = MetaDataUtil.getVersion(result, 0) + 1;
		                    if (newMinVersion > versionConstraint.getMax()) {
		                    	return null;
		                    }
		                    versionConstraint = versionConstraint.min(newMinVersion);
	                		break;
	                	default: throw new RuntimeException("Panic");
	                	}
	                    options = options.versionConstraint(versionConstraint);
	                } else {
	                	return result;
	                }
	            } else {
	            	return null;
	            }
	        }
        } while (failedStore++ < maxFailedStores);
        Log.warning("maxFailedStores exceeded");
        return null;
    }
    
    protected ByteBuffer[] _retrieve(Triple<DHTKey,Integer,Integer>[] keysSegmentNumbersAndIndices, InternalRetrievalOptions options) {
        ByteBuffer[] results;

        results = new ByteBuffer[keysSegmentNumbersAndIndices.length];
        for (int i = 0; i < keysSegmentNumbersAndIndices.length; i++) {
            if (debugVersion) {
                System.out.println("retrieve:\t" + keysSegmentNumbersAndIndices[i].getV1());
                System.out.println("RetrievalOptions:\t" + options);
            }
            if (keysSegmentNumbersAndIndices[i].getV2() == IntCuckooConstants.noSuchValue) {
                results[i] = null;
            }
        }
        readLock.lock();
        try {
            for (int i = 0; i < keysSegmentNumbersAndIndices.length; i++) {
                if (keysSegmentNumbersAndIndices[i].getV2() != IntCuckooConstants.noSuchValue) {
		            if (headSegment.getSegmentNumber() == keysSegmentNumbersAndIndices[i].getV2()) {
		                if (debugSegments) {
		                    Log.warning("Read from head segment");
		                }
		                results[i] = retrieve(headSegment, keysSegmentNumbersAndIndices[i].getV1(), options);
		                if (debugSegments) {
		                    Log.warning("Done read from head segment");
		                }
		            } else {
		                try {
		                    AbstractSegment segment;
		
		                    segment = getSegment(keysSegmentNumbersAndIndices[i].getV2(), readSegmentPrereadMode);
		                    try {
		                        if (debugSegments) {
		                            Log.warning("Read from file segment");
		                        }
		                        results[i] = retrieve(segment, keysSegmentNumbersAndIndices[i].getV1(), options);
		                        if (debugSegments) {
		                            Log.warning("Done read from file segment");
		                            Log.warning("result: " + results[i]);
		                        }
		                    } finally {
		                    	// Not optimizing this as we do not expect to use it at present
		                    	// (we're using the map everything approach at present)
		                        if (nsOptions.getStorageType() == StorageType.FILE) {
		                        	if (segment != headSegment) {
		                        		((FileSegment)segment).removeReference();
		                        	}
		                        }
		                    }
		                } catch (IOException ioe) {
		                    Log.logErrorWarning(ioe);
		                    return null;
		                }
		            }
                }
            }
        } finally {
            readLock.unlock();
        }
        
        for (int i = 0; i < results.length; i++) {
	        if (results[i] != null) {
	            // Double check that the result is valid
	            if (debug) {
	                System.out.printf("tpc %d\n", CCSSUtil.getStorageState(MetaDataUtil.getCCSS(results[i], 0)));
	                System.out.printf("tpc %s\n", StorageProtocolUtil.storageStateValidForRead(nsOptions.getConsistencyProtocol(), 
	                        CCSSUtil.getStorageState(MetaDataUtil.getCCSS(results[i], 0))));
	            }
	            if (!StorageProtocolUtil.storageStateValidForRead(nsOptions.getConsistencyProtocol(), 
	                                           MetaDataUtil.getStorageState(results[i], 0))) {
	                //System.out.printf("key %s storage state: %d\n", KeyUtil.keyToString(key), 
	                //        CCSSUtil.getStorageState(MetaDataUtil.getCCSS(result, 0)));
	            	// If we detect a failed store, simply revert to the single key retrieval code
	            	// which is capable of handling this situation
	                results[i] = _retrieve(keysSegmentNumbersAndIndices[i].getV1(), options);
	            }
	        }
        }
        
        return results;
    }

    /**
     *  Call segment.retrieve() verifying storage state if necessary, and re-issuing a
     *  modified call (with a verification request) if verification fails
     * @param segment
     * @param key
     * @param options
     * @return
     */
    private ByteBuffer retrieve(AbstractSegment segment, DHTKey key, InternalRetrievalOptions options) {
    	try {
	    	ByteBuffer	result;
	    	
	        result = segment.retrieve(key, options);
	        if (result != null && verifyStorageState && !storageStateValid(result)) {
	            result = segment.retrieve(key, options.cpSSToVerify(nsOptions.getConsistencyProtocol()));
	        }
	        return result;
    	} catch (RuntimeException re) {
    		Log.warningf("Due to exception %s, removing %d from recentFileSegments", re, segment.getSegmentNumber());
    		recentFileSegments.remove(segment.getSegmentNumber());
    		throw re;
    	}
	}

	private boolean storageStateValid(ByteBuffer result) {
		return StorageProtocolUtil.storageStateValidForRead(nsOptions.getConsistencyProtocol(), 
                	MetaDataUtil.getStorageState(result, 0));
	}

	public void cleanupPendingWaitFors() {
        if (debugWaitFor) {
        	System.out.println("Cleaning pending waitfors");
        }
        for (Map.Entry<DHTKey, Set<PendingWaitFor>> entry : pendingWaitFors.entrySet()) {
            for (PendingWaitFor pendingWaitFor : entry.getValue()) {
                ActiveProxyRetrieval waiter;

                waiter = activeRetrievals.get(pendingWaitFor.getOpUUID());
                if (waiter == null) {
                    if (debugWaitFor) {
                    	System.out.printf("No active retrieval for %s\n", pendingWaitFor.getKey());
                    } else {
                    	System.out.printf("Found active retrieval for %s\n", pendingWaitFor.getKey());
                    }
                    entry.getValue().remove(pendingWaitFor);
                }
            }
            if (entry.getValue().size() == 0) {
                pendingWaitFors.remove(entry.getKey());
            }
        }
    }

    private int getSegmentNumber(DHTKey key, VersionConstraint vc) {
        metaReadLock.lock();
        try {
            int	rawSegmentNumber;
            int	segmentNumber;
    
            rawSegmentNumber = valueSegments.get(key);
            if (debugSegments) {
                Log.warning("valueSegments.get " + key + " " + rawSegmentNumber);
            }
            if (rawSegmentNumber < 0) {
                if (rawSegmentNumber != IntCuckooConstants.noSuchValue) {
                    OffsetList	offsetList;
    
                    offsetList = offsetListStore.getOffsetList(-rawSegmentNumber);
                    segmentNumber = offsetList.getOffset(vc, null);
                    if (segmentNumber < 0) {
                        if (segmentNumber == OffsetList.NO_MATCH_FOUND) {
                        	segmentNumber = IntCuckooConstants.noSuchValue;
                        } else {
                            throw new RuntimeException("Unexpected segmentNumber: " + segmentNumber);
                        }
                    }
                } else {
                	segmentNumber = IntCuckooConstants.noSuchValue;
                }
            } else {
            	segmentNumber = rawSegmentNumber;
            }
            if (debugSegments) {
                Log.warning("offsetList segmentNumber: ", rawSegmentNumber);
            }
            return segmentNumber;
        } finally {
            metaReadLock.unlock();
        }
    }

    private long[] segmentOldestVersion(int segmentNumber, DHTKey key) {
        return segmentVersion(segmentNumber, key, oldestVersionOptions);
    }

    private long[] segmentNewestVersion(int segmentNumber, DHTKey key) {
        return segmentVersion(segmentNumber, key, newestVersionOptions);
    }

    private long[] segmentVersion(int segmentNumber, DHTKey key, InternalRetrievalOptions retrievalOptions) {
        try {
            ByteBuffer      result;
            AbstractSegment segment;

            // FUTURE - reduce disk accesses
            segment = getSegmentChecked(segmentNumber);
            try {
                return segmentVersionAndStorageTime(segment, key, retrievalOptions);
                /*
                if (debugSegments) {
                    Log.warning("Read segmentVersion");
                }
                result = segment.retrieve(key, retrievalOptions);
                if (debugSegments) {
                    Log.warning("Done read segmentVersion");
                    Log.warning("result: "+ result);
                }
                if (result != null) {
                    return MetaDataUtil.getVersion(result, 0);
                } else {
                    return noSuchVersion;
                }
                */
            } finally {
                if (nsOptions.getStorageType() == StorageType.FILE) {
                	if (segment != headSegment) {
                		((FileSegment)segment).removeReference();
                	}
                }
            }
        } catch (IOException ioe) {
            Log.logErrorWarning(ioe);
            throw new RuntimeException(ioe);
        }
    }

    private long[] segmentVersionAndStorageTime(AbstractSegment segment, DHTKey key, InternalRetrievalOptions retrievalOptions) {
        ByteBuffer result;
        long[]  versionAndStorageTime;

        versionAndStorageTime = new long[2];
        if (debugSegments) {
            Log.warning("Read segmentVersion");
        }
        result = retrieve(segment, key, retrievalOptions);
        if (debugSegments) {
            Log.warning("Done read segmentVersion");
            Log.warning("result: " + result);
        }
        if (result != null) {
            versionAndStorageTime[VERSION_INDEX] = MetaDataUtil.getVersion(result, 0);
            versionAndStorageTime[STORAGE_TIME_INDEX] = MetaDataUtil.getCreationTime(result, 0);
        } else {
            versionAndStorageTime[VERSION_INDEX] = noSuchVersion;
            versionAndStorageTime[STORAGE_TIME_INDEX] = 0;
        }
        return versionAndStorageTime;
    }

    private WritableSegmentBase getSegment(int segmentNumber, SegmentPrereadMode segmentPrereadMode) throws IOException {
    	if (segmentNumber == headSegment.getSegmentNumber()) {
    		return headSegment;
    	} else {
	        switch (nsOptions.getStorageType()) {
	        case RAM: return ramSegments.get(segmentNumber);
	        case FILE: return getFileSegment(segmentNumber, segmentPrereadMode);
	        default: throw new RuntimeException("Panic");
	        }
    	}
    }
    
    private WritableSegmentBase getSegmentChecked(int segmentNumber) throws IOException {
    	if (headSegment != null && segmentNumber == headSegment.getSegmentNumber()) {
    		return headSegment;
    	} else {
	        switch (nsOptions.getStorageType()) {
	        case RAM: return ramSegments.get(segmentNumber);
	        case FILE: return getFileSegment(segmentNumber, readSegmentPrereadMode);
	        default: throw new RuntimeException("Panic");
	        }
    	}
    }
    
    private boolean segmentExists(int segmentNumber) {
    	if (deletedSegments.contains(segmentNumber)) {
    		return false;
    	} else {
	    	if (segmentNumber < nextSegmentID.get()) {
	    		boolean	exists;
	    		
		        switch (nsOptions.getStorageType()) {
		        case RAM: exists = true; break;
		        case FILE: exists = fileSegmentExists(segmentNumber); break;
		        default: throw new RuntimeException("Panic");
		        }
		        if (!exists) {
		        	deletedSegments.add(segmentNumber);
		        }
		        return exists;
	    	} else {
	    		return false;
	    	}
    	}
    }
    
    private boolean fileSegmentExists(int segmentNumber) {
    	return FileSegment.fileForSegment(nsDir, segmentNumber).exists();
	}

	private FileSegment getFileSegment(int segmentNumber, SegmentPrereadMode segmentPrereadMode) throws IOException {
        try {
        	return _getFileSegment(segmentNumber, segmentPrereadMode);
        } catch (IOException | RuntimeException e) {
        	Exception	lastE;
            int			attemptIndex;
        
            lastE = e;
            attemptIndex = 1;
            Log.warning(String.format("Treating as non fatal. %s ns %x segmentNumber %d attemptIndex %d", e, ns, segmentNumber, attemptIndex));
            while (attemptIndex < fileSegmentAttempts) {
            	try {
                	return _getFileSegment(segmentNumber, segmentPrereadMode);
                } catch (IOException | RuntimeException e2) {
                    lastE = e2;
                    Log.warning(String.format("Treating as non fatal. %s ns %x segmentNumber %d attemptIndex %d", e2, ns, segmentNumber, attemptIndex));
            	}
            	ThreadUtil.sleep(fileSegmentAttemptDelayMillis);
            	attemptIndex++;
            }
            if (lastE instanceof RuntimeException) {
            	throw (RuntimeException)lastE;
            } else {
            	throw (IOException)lastE;
            }
        }
    }
    
    private FileSegment _getFileSegment(int segmentNumber, SegmentPrereadMode segmentPrereadMode) throws IOException {
        FileSegment fileSegment;

        // FUTURE - this implementation touches disk more than we need to
        fileSegment = recentFileSegments.get(segmentNumber);
        if (fileSegment == null) {
            try {
            	FileSegment.SyncMode	syncMode;

            	syncMode = nsOptions.getStorageType() == StorageType.FILE_SYNC ? FileSegment.SyncMode.Sync : FileSegment.SyncMode.NoSync; 
                //fileSegment = FileSegment.openReadOnly(nsDir, segmentNumber, nsOptions.getSegmentSize(), nsOptions);
                fileSegment = FileSegment.openForDataUpdate(nsDir, segmentNumber, nsOptions.getSegmentSize(), syncMode, nsOptions, 
                											segmentIndexLocation, segmentPrereadMode);
            } catch (IOException | OutOfMemoryError e) {
                boolean oom;
                
                if (e instanceof IOException) {
                    oom = e.getCause() != null && (e.getCause() instanceof OutOfMemoryError);
                } else {
                    oom = true;
                }
                if (oom) {
                    Log.warning("OOM attempting to open mapped file. Calling gc and finalization.");
                    System.gc();
                    Log.warning("GC complete.");
                    System.runFinalization();
                    Log.warning("Finalization complete.");
                    fileSegment = FileSegment.openReadOnly(nsDir, segmentNumber, nsOptions.getSegmentSize(), nsOptions);
                } else {
                    throw e;
                }
            }
            fileSegment.addReferences(2); // 1 for the map, 1 for the returned reference
            recentFileSegments.put(segmentNumber, fileSegment);
        }
        if (debugSegments) {
            fileSegment.displayForDebug();
        }
        return fileSegment;
    }

    private class FileSegmentMap extends LinkedHashMap<Integer, FileSegment> {
        private final int capacity;
        private final Lock readLock;
        private final Lock writeLock;
        private final ReadWriteLock rwLock;

        private static final float loadFactor = 0.75f;

        FileSegmentMap(int capacity) {
            super(capacity, loadFactor, true);
            this.capacity = capacity;
            rwLock = new ReentrantReadWriteLock();
            readLock = rwLock.readLock();
            writeLock = rwLock.writeLock();
        }

        @Override
        public FileSegment get(Object segmentNumber) {
            readLock.lock();
            try {
                FileSegment segment;

                segment = super.get(segmentNumber);
                if (segment != null) {
                    // both threads could get to here, but the other thread couldn't remove the reference
                    if (!segment.addReference()) {
                        segment = null;
                    }
                }
                return segment;
            } finally {
                readLock.unlock();
            }
        }

        @Override
        public FileSegment put(Integer segmentNumber, FileSegment fileSegment) {
            writeLock.lock();
            try {
                FileSegment prev;

                prev = super.put(segmentNumber, fileSegment);
                if (size() > capacity) {
                    Map.Entry<Integer, FileSegment> eldest;
                    Iterator<Map.Entry<Integer, FileSegment>> iterator;

                    iterator = entrySet().iterator();
                    if (iterator.hasNext()) {
                        eldest = iterator.next();
                        remove(eldest.getKey()); // first remove so that nobody else will get a reference
                        // eldest.getValue().close();
                        eldest.getValue().removeReference();
                    }
                }
                return prev;
            } finally {
                writeLock.unlock();
            }
        }

        @Override
        public void putAll(Map<? extends Integer, ? extends FileSegment> m) {
            throw new UnsupportedOperationException();
        }
        
        public FileSegment remove(Integer segmentNumber) {
            writeLock.lock();
            try {
                FileSegment prev;

                prev = super.remove(segmentNumber);
                if (prev != null) {
                	prev.removeReference();
                }
                return prev;
            } finally {
                writeLock.unlock();
            }
        }        
    }

    static NamespaceStore recoverExisting(long ns, File nsDir, NamespaceStore parent, StoragePolicy storagePolicy, 
            MessageGroupBase mgBase,
            NodeRingMaster2 ringMaster, ConcurrentMap<UUIDBase, ActiveProxyRetrieval> activeRetrievals,
            ZooKeeperExtended zk, String nsLinkBasePath, LinkCreationListener linkCreationListener) {
        NamespaceStore nsStore;
        NamespaceProperties nsProperties;
        int	numSegmentsToPreread;
        int	numSegmentsToSkipPreread;
        int	segmentsPrereadSkipped;

        if (ns == NamespaceUtil.metaInfoNamespace.contextAsLong()) {
            nsProperties = NamespaceUtil.metaInfoNamespaceProperties;
        } else {
            try {
                nsProperties = NamespacePropertiesIO.read(nsDir);
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
        nsStore = new NamespaceStore(ns, nsDir, NamespaceStore.DirCreationMode.DoNotCreateNSDir, 
                                     nsProperties, parent, mgBase, ringMaster, true, activeRetrievals);
        if (nsProperties.getOptions().getStorageType() != StorageType.RAM) {
            List<Integer> segmentNumbers;
        	
        	segmentNumbers = FileUtil.numericFilesInDirAsSortedIntegerList(nsDir);
        	if (segmentNumbers.size() > 0) {
	            FileSegmentRecoverer fsr;
	            int headSegmentNumber;
	            int headSegmentNumberIndex;
	
	            fsr = new FileSegmentRecoverer(nsDir);
	            
	            headSegmentNumberIndex = segmentNumbers.size() - 1;
	            headSegmentNumber = segmentNumbers.get(headSegmentNumberIndex); 
	            segmentNumbers.remove(headSegmentNumberIndex);
	            
	            numSegmentsToPreread = (int)((long)nsPrereadGB * (1024L * 1024L * 1024L) / (long)(nsProperties.getOptions().getSegmentSize()));
	            numSegmentsToSkipPreread = segmentNumbers.size() - numSegmentsToPreread; 
	            segmentsPrereadSkipped = 0;
	            Log.warningf("segmentsToPreread: %d", numSegmentsToPreread);
	            
	            for (int i : segmentNumbers) {
            		FileSegment	segment;
    	            SegmentPrereadMode	segmentPrereadMode;
            		
    	            segmentPrereadMode = segmentsPrereadSkipped < numSegmentsToSkipPreread ? SegmentPrereadMode.NoPreread : SegmentPrereadMode.Preread;
            		segment = null;
	            	try {
	            		segment = fsr.recoverFullSegment(i, nsStore, segmentIndexLocation, segmentPrereadMode);
	            	} catch (Exception e) {
	            		Log.logErrorWarning(e, "Error recovering full segment "+ i);
	            		Log.warningf("Attempting partial segment recovery of segment: %d %s", i, segmentPrereadMode);
	            		segment = fsr.readPartialSegment(i, false);
	            		Log.warning("Successfully read segment as partial segment: "+ i);
	            		Log.warning("Persisting segment: "+ i);
	            		try {
	            			segment.persist();
	            		} catch (IOException ioe) {
	            			throw new RuntimeException("Segment persists failed for recovered segment" + i, ioe);
	            		}
	            		Log.warning("Persisted segment: "+ i);
	            		Log.warning("Resuming full segment recovery: "+ i);
	            		segment = fsr.recoverFullSegment(i, nsStore, segmentIndexLocation, segmentPrereadMode);
	            	}
	            	if (segmentPrereadMode == SegmentPrereadMode.Preread && segment != null) {
	            		nsStore.addRecentFileSegment(i, segment);
	            	}
	            	segmentsPrereadSkipped++;
	            }
	            // Check for partial recovery on last
	            // FUTURE - Check for corruption
	            // FUTURE - If full, then do full recovery?
	            nsStore.setHeadSegment(fsr.recoverPartialSegment(headSegmentNumber, nsStore));
        	} else {
        		nsStore.createInitialHeadSegment();
        	}
        } else {
        	nsStore.initRAMSegments();
        }
        return nsStore;
    }
    
    private void addRecentFileSegment(int segmentNumber, FileSegment fileSegment) {
        fileSegment.addReferences(2); // 1 for the map, 1 for the returned reference
        recentFileSegments.put(segmentNumber, fileSegment);
    }

    // FUTURE - Consider deco
    public OpResult snapshot(long version) {
        writeLock.lock();
        try {
            if (version >= curSnapshot) {
                curSnapshot = version;
                return OpResult.SUCCEEDED;
            } else {
                return OpResult.INVALID_VERSION;
            }
        } finally {
            writeLock.unlock();
        }
    }

    ////////////////
    // convergence
    
    public boolean getChecksumTreeForLocal(UUIDBase uuid, ConvergencePoint targetCP, ConvergencePoint sourceCP, 
            							MessageGroupConnection connection, byte[] originator, RingRegion region,
            							IPAndPort replica, int timeoutMillis) {
    	ActiveRegionSync	ars;
    	
    	ars = new ActiveRegionSync(this, checksumTreeServer, mgBase, new ChecksumTreeRequest(targetCP, sourceCP, region, replica));
    	Log.warningAsyncf("getChecksumTreeForLocal req uuid %s  ars uuid %s  %s %s %s %s", uuid, ars.getUUID(), targetCP, sourceCP, region, replica);
    	activeRegionSyncs.put(ars.getUUID(), ars);
    	try {
	    	// Forward request to remote
	    	ars.startSync();
	    	// Wait for completion
	    	// Pass result back
	    	return ars.waitForCompletion(timeoutMillis, TimeUnit.MILLISECONDS);
    	} finally {
        	activeRegionSyncs.remove(ars.getUUID());
    	}
    }
    
    public void getChecksumTreeForRemote(UUIDBase uuid, ConvergencePoint targetCP, ConvergencePoint sourceCP, 
                                         MessageGroupConnection connection, byte[] originator, RingRegion region) {
        checksumTreeServer.getChecksumTree(uuid, targetCP, sourceCP, connection, originator, region);
    }

    public void incomingChecksumTree(UUIDBase uuidBase, ChecksumNode remoteTree, ConvergencePoint cp, 
                                     MessageGroupConnection connection) {
    	ActiveRegionSync	ars;
    	
    	Log.warningAsyncf("incomingChecksumTree %s", uuidBase);
    	ars = activeRegionSyncs.get(uuidBase);
    	if (ars == null) {
    		Log.warningf("Ignoring unexpected incoming checksum tree %x %s", ns, uuidBase.toString());
    	} else {
    		ars.incomingChecksumTree(cp, remoteTree, connection);
    	}
    }

    public void incomingSyncRetrievalResponse(MessageGroup message) {
        message = message.ensureArrayBacked();
        ActiveRegionSync._incomingSyncRetrievalResponse(message);
    }
    
    public void handleSecondarySync(MessageGroup message) {
        List<StorageValueAndParameters> svpList;
        
        if (debug) {
        	Log.warningAsyncf("incomingSyncRetrievalResponse");
        }
        svpList = new ArrayList<>();
        for (MessageGroupRetrievalResponseEntry entry : message.getRetrievalResponseValueKeyIterator()) {
            StorageValueAndParameters   svp;
            
            svp = StorageValueAndParameters.createSVP(entry);
            if (svp != null) {
                svpList.add(svp);
            }
        }
        if (!svpList.isEmpty()) {
            put(svpList, emptyUserData, NullKeyedOpResultListener.instance);
        }
    }
    

    // LOCKING WILL NEED TO BE HANDLED IF BELOW IS EXPOSED
    //public Iterator<KeyValueChecksum> keyValueChecksumIterator(long version) {
    //    return new KeyValueChecksumIterator(version);
    ///}

    private class KeyValueChecksumIterator implements Iterator<KeyValueChecksum> {
        private final Iterator<DHTKeyIntEntry> valueSegmentEntries;

        private final RetrievalOptions retrievalOptions;

        private KeyValueChecksumIterator(long version) {
            valueSegmentEntries = valueSegments.iterator();
            retrievalOptions = OptionsHelper.newRetrievalOptions(RetrievalType.VALUE_AND_META_DATA, WaitMode.GET,
                    VersionConstraint.greatest);
        }

        @Override
        public boolean hasNext() {
            return valueSegmentEntries.hasNext();
        }

        @Override
        public KeyValueChecksum next() {
            DHTKeyIntEntry vsEntry;
            ByteBuffer result;
            byte[] checksum;

            vsEntry = valueSegmentEntries.next();
            result = _retrieve(vsEntry, retrievalOptions);
            // FUTURE think about above in light of new retrieval grouping
            // checksum = new byte[checksumType.length()];

            checksum = MetaDataUtil.getChecksum(result, 0);
            // result.position(0);
            // result.get(checksum);
            return new KeyValueChecksum(vsEntry.getKey(), checksum);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    
    // FIXME - reminder: convergence implementation is currently only valid for SINGLE_VERSION
    
    /** readLock() must be held while this is in use and readUnlock() must be called when complete */
    public Iterator<KeyAndVersionChecksum> keyAndVersionChecksumIterator(long minVersion, long maxVersion) {
        return new KeyAndVersionChecksumIterator(minVersion, maxVersion);
    }

    private class KeyAndVersionChecksumIterator implements Iterator<KeyAndVersionChecksum> {
        private final Iterator<DHTKeyIntEntry>  valueSegmentEntries;
        private KeyAndVersionChecksum   next;

        private final RetrievalOptions retrievalOptions;

        private KeyAndVersionChecksumIterator(long minVersion, long maxVersion) {
            valueSegmentEntries = valueSegments.iterator();
            retrievalOptions = OptionsHelper.newRetrievalOptions(RetrievalType.VALUE_AND_META_DATA, WaitMode.GET,
                    new VersionConstraint(minVersion, maxVersion, Mode.GREATEST));
                    //VersionConstraint.greatest);
            moveToNext();
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        private void moveToNext() {
            KeyAndVersionChecksum   _next;
            
            _next = null;
            while (_next == null && valueSegmentEntries.hasNext()) {
                DHTKeyIntEntry 	vsEntry;
                long			checksum;
                boolean			validChecksum;
                
                vsEntry = valueSegmentEntries.next();
                if (nsProperties.getOptions().isWriteOnce()) {
                	checksum = 0;
                	validChecksum = true;
                } else {
                	MultiVersionChecksum	_checksum;
                	
                	try {
                		_checksum = getVersionChecksum(vsEntry.getKey());
                	} catch (RuntimeException re) {
                		re.printStackTrace();
                		_checksum = null;
                	}
                	if (_checksum != null) {
                		checksum = _checksum.getLongChecksum();
                    	validChecksum = true;
                	} else {
                		checksum = 0;
                    	validChecksum = false;
                	}
                }
                if (validChecksum) {
                	_next = new KeyAndVersionChecksum(vsEntry.getKey(), checksum);
                }
                /*
                ByteBuffer result;
                long checksum;
                 FUTURE - support bitemporal
                 for now we have commented out checksum retrieval as we are not
                 using the checksum
                result = _retrieve(vsEntry, retrievalOptions);
                // FUTURE think about above in light of new retrieval grouping
                // checksum = new byte[checksumType.length()];
    
                if (result != null) {
                    checksum = MetaDataUtil.getVersion(result, 0);
                    
                    //System.out.printf("%s\n", StringUtil.byteBufferToHexString(result));
                    //System.out.printf("%x\t%d\n", checksum, checksum);
                    // FUTURE - need to handle this for multi-versioned values
        
                    // checksum = MetaDataUtil.getChecksum(result, 0);
                    // result.position(0);
                    // result.get(checksum);
                    _next = new KeyAndVersionChecksum(vsEntry.getKey(), checksum);
                }
                */
            }
            next = _next;
        }
        
        @Override
        public KeyAndVersionChecksum next() {
            KeyAndVersionChecksum   _next;
            
            _next = next;
            moveToNext();
            return _next;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
    
    private MultiVersionChecksum getVersionChecksum(DHTKey key) {
    	int		segmentNumber;
    	
    	segmentNumber = valueSegments.get(key);
    	if (segmentNumber >= 0) {
            MultiVersionChecksum	checksum;
            WritableSegmentBase		segment;
            long					latestVersion;
            
            try {
            	segment = getSegment(segmentNumber, SegmentPrereadMode.Preread);
            } catch (IOException ioe) {
            	throw new RuntimeException(ioe);
            }
			try {
				int	offset;
				
				offset = segment.getRawOffset(key);
				if (offset < 0) {
					throw new RuntimeException("Unexpected offset < 0: "+ key +" "+ offset +" "+ segmentNumber);
				}
				latestVersion = segment.getVersion(offset);
    		} finally {
                if (nsOptions.getStorageType() == StorageType.FILE) {
                	if (segment != headSegment) {
                		((FileSegment)segment).removeReference();
                	}
                }
    		}
            checksum = new MultiVersionChecksum();
            checksum.addKey(key);
            checksum.addVersionAndStorageTime(latestVersion, 0);
            return checksum;
    		//return MultiVersionChecksum.fromKey(key);            
    	} else {
            OffsetList 			offsetList;
            
            offsetList = offsetListStore.getOffsetList(-segmentNumber);
            //return offsetList.getMultiVersionChecksum();
            // FIXME - TEMPORARY - ONLY CONSIDER THE MOST RECENT VALUE
            // FOR CONVERGENCE
            MultiVersionChecksum	checksum;
            long					latestVersion;
            
            latestVersion = offsetList.getLatestVersion();
            if (latestVersion >= 0) {
	            checksum = new MultiVersionChecksum();
	            checksum.addKey(key);
	            checksum.addVersionAndStorageTime(latestVersion, 0);
	            return checksum;
            } else {
            	return null;
            }
    	}
    }
    
    public void readLock() {
        readLock.lock();
        metaReadLock.lock();
    }
    
    public void readUnlock() {
        metaReadLock.unlock();
        readLock.unlock();
    }
    
    ////////////////////
    // Retention
    
    public void reap(boolean leaveTrash) {
    	ValueRetentionPolicy	vrp;
    	
    	vrp = nsOptions.getValueRetentionPolicy();
    	Log.warningAsyncf("reap ns %x %s %s", ns, vrp, leaveTrash);
    	if (vrp != null) {
    		if (!leaveTrash) {
    			FileSegmentCompactor.emptyTrashAndCompaction(nsDir);
    		}
	    	writeLock.lock();
	    	metaWriteLock.lock();
	    	try {
	        	long	curTimeNanos;
	        	
	        	curTimeNanos = systemTimeSource.absTimeNanos();
		    	switch (vrp.getImplementationType()) {
		    	case SingleReverseSegmentWalk:
		    		singleReverseSegmentWalk(vrp, vrp.createInitialState(), curTimeNanos);
		    		break;
		    	case RetainAll:
		    		break;
				default: throw new RuntimeException("Unsupported ValueRetentionPolicy ImplementationType: "+ vrp.getImplementationType());
		    	}
	    	} finally {
		    	metaWriteLock.unlock();
		    	writeLock.unlock();
	    	}
    		if (!leaveTrash) {
    			FileSegmentCompactor.emptyTrashAndCompaction(nsDir);
    		}
    	}
    }
    
	public <T extends ValueRetentionState> void singleReverseSegmentWalk(ValueRetentionPolicy<T> vrp, T valueRetentionState, long curTimeNanos) {
		Set<Integer>			deletedSegments;
		HashedSetMap<DHTKey,Triple<Long,Integer,Long>>	removedEntries;
		
		removedEntries = new HashedSetMap<>();
		deletedSegments = new HashSet<>();
        for (int i = headSegment.getSegmentNumber() - 1; i >= 0; i--) {
        	if (segmentExists(i)) {
        		Triple<CompactionCheckResult,Set<Integer>,Set<Integer>>	result; 
        		WritableSegmentBase		segment;
        		CompactionCheckResult	ccr;
        		
        		try {
                	Stopwatch	sw;
                	
                	sw = new SimpleStopwatch();
        			if (nsOptions.getStorageType() == StorageType.RAM) {
        				segment = getSegment(i, SegmentPrereadMode.Preread);
        			} else {
        				segment = getSegment(i, SegmentPrereadMode.Preread);
        				//segment = FileSegment.openReadOnly(nsDir, i, nsOptions.getSegmentSize(), nsOptions, SegmentIndexLocation.RAM, SegmentPrereadMode.Preread);
        			}
					try {
						result = segment.singleReverseSegmentWalk(vrp, valueRetentionState, curTimeNanos, ringMaster);
						ccr = result.getV1();
		    		} finally {
		                if (nsOptions.getStorageType() == StorageType.FILE) {
		                	if (segment.getSegmentNumber() != headSegment.getSegmentNumber()) {
		                		((FileSegment)segment).removeReference();
		                	}
		                }
		    		}
					Log.warningAsyncf("Segment %3d CompactionCheckResult:\t%s", i, ccr.toString());
					if (segment.getSegmentNumber() == headSegment.getSegmentNumber()) {
						Log.warningAsyncf("Retaining head segment");
					} else {
						if (ccr.getValidEntries() == 0) {
			            	try {
			            		recentFileSegments.remove(i);
			            		FileSegmentCompactor.delete(nsDir, i);
			            		deletedSegments.add(i);
			            		if (FileSegment.mapEverything) {
			                		((FileSegment)segment).close();
			            		}
			            	} catch (IOException ioe) {
			            		Log.logErrorWarning(ioe, "Failed to delete segment: "+ i);
			            	}
						} else if (ccr.getInvalidFraction() >= compactionThreshold) {
			            	try {
								HashedSetMap<DHTKey,Triple<Long,Integer,Long>>	segmentRemovedEntries;
								
			            		recentFileSegments.remove(i);
			            		segmentRemovedEntries = FileSegmentCompactor.compact(nsDir, i, nsOptions, new RetainedOffsetMapCheck(result.getV2(), result.getV3()));
			            		removedEntries.addAll(segmentRemovedEntries);
			            	} catch (IOException ioe) {
			            		Log.logErrorWarning(ioe, "IOException compacting segment: "+ i);
			            	}
						}
					}
					sw.stop();
					Log.warningAsyncf("\t\t%d %f", i, sw.getElapsedSeconds());
				} catch (Exception e) {
					Log.logErrorWarning(e, "Skipping segment "+ i +" due to Exception");
				}
        	}
        }
        updateOffsetLists(deletedSegments, removedEntries);
    }

    private void updateOffsetLists(Set<Integer> deletedSegments, HashedSetMap<DHTKey, Triple<Long, Integer, Long>> removedEntries) {
    	RAMOffsetListStore	ols;
    	
    	ols = (RAMOffsetListStore)offsetListStore;
    	
    	if (deletedSegments.size() > 0) {
	    	Set<DHTKey>	singleKeysInDeletedSegments;
	    	
	    	for (int i = 1; i <= ols.getNumLists(); i++) { // offset list indexing is 1-based
	    		RAMOffsetList	ol;
	    		
	    		ol = (RAMOffsetList)ols.getOffsetList(i);
	    		ol.removeEntriesByValue(deletedSegments);
	    	}
	    	singleKeysInDeletedSegments = new HashSet<>();
	    	for (DHTKeyIntEntry keyAndSegment : valueSegments) {
	    		if (keyAndSegment.getValue() >= 0 && deletedSegments.contains(keyAndSegment.getValue())) {
	    			singleKeysInDeletedSegments.add(keyAndSegment.getKey());
	    		}
	    	}
	    	for (DHTKey singleKeyToRemove : singleKeysInDeletedSegments) {
	    		valueSegments.remove(singleKeyToRemove);
	    	}
    	}
    	
    	for (DHTKey key : removedEntries.getKeys()) {
            int				rawSegmentNumber;
    
            rawSegmentNumber = valueSegments.get(key);
            if (rawSegmentNumber < 0) {
                if (rawSegmentNumber != IntCuckooConstants.noSuchValue) {
            		RAMOffsetList	ol;
            		
            		ol = (RAMOffsetList)ols.getOffsetList(-rawSegmentNumber);
            		ol.removeEntriesByMatch(removedEntries.getSet(key));
                } else {
                	// No action required
                }
            } else {
                long[]	versionAndStorageTime;
                long	creationTime;

                versionAndStorageTime = segmentOldestVersion(rawSegmentNumber, key); // only one should exist
                if (removedEntries.getSet(key).size() != 1) {
                	Log.warningAsyncf("Unexpected removedEntries.getSet(key).size() != 1");
                }
                creationTime = nsOptions.getRevisionMode() == RevisionMode.UNRESTRICTED_REVISIONS ? versionAndStorageTime[1] : 0;
                if (removedEntries.getSet(key).contains(new Triple<>(versionAndStorageTime[0], rawSegmentNumber, creationTime))) {
                	valueSegments.remove(key);
                } else {
                	Log.warningAsyncf("Unexpected !removedEntries.getSet(key).contains(new Triple<>(versionAndStorageTime[0], rawSegmentNumber, creationTime))");
                	Log.warningAsyncf("%s", new Triple<>(versionAndStorageTime[0], rawSegmentNumber, creationTime));
                }
            }    		
    	}
	}
    
    ////////////////////////////////////
    // SSNamespaceStore implementation

	@Override
	public long getNamespaceHash() {
		return ns;
	}

	@Override
	public boolean isNamespace(String ns) {
		return NamespaceUtil.nameToLong(ns) == this.ns;
	}

	@Override
	public OpResult put(DHTKey key, ByteBuffer value, SSStorageParameters storageParams, byte[] userData, NamespaceVersionMode nsVersionMode) {
	    return _put(key, value, StorageParameters.fromSSStorageParameters(storageParams), userData, nsVersionMode);
	}

	@Override
	public ByteBuffer retrieve(DHTKey key, SSRetrievalOptions options) {
	    return _retrieve(key, InternalRetrievalOptions.fromSSRetrievalOptions(options));
	}

	@Override
	public File getNamespaceSSDir() {
		synchronized (ssDir) {
			if (!ssDir.exists() && !ssDir.mkdir()) {
				throw new RuntimeException("Unable to create: "+ ssDir);
			} else {
				return ssDir;
			}
		}
	}
}
