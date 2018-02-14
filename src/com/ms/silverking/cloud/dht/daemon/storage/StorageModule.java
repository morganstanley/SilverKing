package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.client.impl.SimpleNamespaceCreator;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.common.NamespaceMetaStore;
import com.ms.silverking.cloud.dht.common.NamespaceMetaStore.NamespaceOptionsRetrievalMode;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.common.NamespaceUtil;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.SimpleValueCreator;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.ActiveProxyRetrieval;
import com.ms.silverking.cloud.dht.daemon.DHTNodeConfiguration;
import com.ms.silverking.cloud.dht.daemon.NodeRingMaster2;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ChecksumNode;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ConvergencePoint;
import com.ms.silverking.cloud.dht.meta.LinkCreationListener;
import com.ms.silverking.cloud.dht.meta.MetaPaths;
import com.ms.silverking.cloud.dht.meta.NodeInfoZK;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.net.MessageGroupConnection;
import com.ms.silverking.cloud.dht.net.ProtoChecksumTreeMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoNamespaceResponseMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoOpResponseMessageGroup;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.cloud.storagepolicy.StoragePolicyGroup;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.thread.lwt.BaseWorker;
import com.ms.silverking.thread.lwt.LWTPool;
import com.ms.silverking.thread.lwt.LWTPoolParameters;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.thread.lwt.LWTThreadUtil;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;
import com.ms.silverking.time.TimeUtils;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.memory.JVMMonitor;

public class StorageModule implements LinkCreationListener {
    private final NodeRingMaster2    ringMaster;
    private final ConcurrentMap<Long,NamespaceStore>    namespaces;
    private final File  baseDir;
    private final NamespaceMetaStore    nsMetaStore;
    private MessageGroupBase    mgBase;
    private StoragePolicyGroup  spGroup;
    private ConcurrentMap<UUIDBase,ActiveProxyRetrieval>  activeRetrievals;
    private NodeNamespaceStore    nodeNSStore;
    private SystemNamespaceStore	systemNSStore;
    private ReplicasNamespaceStore  replicasNSStore;
    private Lock    nsCreationLock;
    private final ZooKeeperExtended zk;
    private final String            nsLinkBasePath;
    private final MethodCallWorker  methodCallWorker;
    private final LWTPool           methodCallPool;
    private final Timer				timer;
    private final ValueCreator      myOriginatorID;
    private final NodeInfoZK		nodeInfoZK;
    
    private NamespaceStore  metaNamespaceStore; // used to bootstrap the meta NS store
                                                // reference held here merely to ensure no GC
    
    private static final int    sessionTimeoutMillis = 5 * 60 * 1000;
    
    private static final int    cleanupPeriodMillis = 5 * 1000;
    private static final int    defaultReapPeriodSeconds = TimeUtils.MINUTES_PER_HOUR * TimeUtils.SECONDS_PER_MINUTE;
    private static final int    reapPeriodMillis;
    private static final int    reapMaxInitialDelayMillis = 1 * 60 * 1000;
    private static final int    reapInitialDelayMillis;
    
    //private static final int    primaryConvergencePeriodMillis = 60 * 1000;
    //private static final int    secondaryConvergencePeriodMillis = 60 * 1000;
    private static final int    primaryConvergencePeriodMillis = 20 * 60 * 1000;
    private static final int    secondaryConvergencePeriodMillis = 5 * 60 * 1000;
    private static final int    convergenceStartupDelayMillis = 30 * 1000;
    //private static final int    convergencePeriodMillis = 10 * 60 * 1000;
    //private static final int    convergenceStartupDelayMillis = 1 * 60 * 1000;
    // FUTURE - add notion of idle DHT and converge more aggressively during idle periods
    //          and far less aggressively during busy periods
    
    private static final boolean    debugLink = false;
    private static final boolean    debugNSRequests = true;

    // FUTURE - this version evolved from a proof-of-concept implementation
    // could consider a revamped approach in the future, or could continue
    // to evolve this implementation to improve performance and features.
    // see Persistence.doc
    // think about renaming ValueStore if we don't have a different
    // ValueStore class after implementing all of Persistence.doc ideas
    
    private enum NSCreationMode {CreateIfAbsent, DoNotCreate};
    public enum RetrievalImplementation {Ungrouped, Grouped};
    
    private static final Set<Long>			dynamicNamespaces = new HashSet<>();
    
    private static final RetrievalImplementation	retrievalImplementation;
    
    static {
    	reapPeriodMillis = PropertiesHelper.systemHelper.getInt(DHTConstants.reapIntervalProperty, defaultReapPeriodSeconds) * 1000;
    	reapInitialDelayMillis = Math.min(reapPeriodMillis, reapMaxInitialDelayMillis);
    	Log.warningf("reapInitialDelayMillis:\t%d", reapInitialDelayMillis);
    	Log.warningf("reapPeriodMillis:\t%d", reapPeriodMillis);
    	retrievalImplementation = RetrievalImplementation.valueOf(
    			PropertiesHelper.systemHelper.getString(DHTConstants.retrievalImplementationProperty, DHTConstants.defaultRetrievalImplementation.toString()));
    	Log.warningf("retrievalImplementation: %s", retrievalImplementation);
    }
    
    public StorageModule(NodeRingMaster2 ringMaster, String dhtName, Timer timer, ZooKeeperConfig zkConfig, NodeInfoZK nodeInfoZK) {
        ClientDHTConfiguration  clientDHTConfiguration;
        
        this.timer = timer;
        this.ringMaster = ringMaster;
        this.nodeInfoZK = nodeInfoZK;
        ringMaster.setStorageModule(this);
        namespaces = new ConcurrentHashMap<>();
        baseDir = new File(DHTNodeConfiguration.dataBasePath, dhtName);
        clientDHTConfiguration = new ClientDHTConfiguration(dhtName, zkConfig);
        nsMetaStore = NamespaceMetaStore.create(clientDHTConfiguration);
        //spGroup = createTestPolicy();
        spGroup = null;
        myOriginatorID = SimpleValueCreator.forLocalProcess();
        // FIXME commented out temporarily
        // We may need to ensure that this runs during relatively quiet times
        /*
        timer.scheduleAtFixedRate(new ConvergenceChecker(OwnerQueryMode.Primary), 
                    ThreadLocalRandom.current().nextInt(convergenceStartupDelayMillis, 
                                                        primaryConvergencePeriodMillis * 2), 
                    primaryConvergencePeriodMillis);
        timer.scheduleAtFixedRate(new ConvergenceChecker(OwnerQueryMode.Primary), 
                    ThreadLocalRandom.current().nextInt(convergenceStartupDelayMillis, 
                                                        secondaryConvergencePeriodMillis * 2), 
                    secondaryConvergencePeriodMillis);
                    */
        methodCallPool = LWTPoolProvider.createPool(LWTPoolParameters.create(methodCallPoolName)
                .maxSize(methodCallPoolMaxSize).targetSize(methodCallPoolTargetSize)
                .workUnit(methodCallPoolWorkUnit).commonQueue(true));
        methodCallWorker = new MethodCallWorker(methodCallPool, this);
        Log.warning("methodCallPool created");
        
        nsCreationLock = new ReentrantLock();
        
        try {
            zk = new ZooKeeperExtended(zkConfig, sessionTimeoutMillis, null);
            nsLinkBasePath = MetaPaths.getInstanceNSLinkPath(dhtName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public void setMessageGroupBase(MessageGroupBase mgBase) {
        this.mgBase = mgBase;
    }
    
    public void setActiveRetrievals(ConcurrentMap<UUIDBase,ActiveProxyRetrieval> activeRetrievals) {
        this.activeRetrievals = activeRetrievals;
    }
    
    public void setReady() {
        // meta ns
        createMetaNSStore();
        // node ns
        nodeNSStore = new NodeNamespaceStore(mgBase, ringMaster, activeRetrievals, namespaces.values());
        addDynamicNamespace(nodeNSStore);
        // replicas ns
        replicasNSStore = new ReplicasNamespaceStore(mgBase, ringMaster, activeRetrievals);
        addDynamicNamespace(replicasNSStore);
        // system ns
        systemNSStore = new SystemNamespaceStore(mgBase, ringMaster, activeRetrievals, namespaces.values(), nodeInfoZK);
        addDynamicNamespace(systemNSStore);
        
        timer.scheduleAtFixedRate(new Cleaner(), cleanupPeriodMillis, cleanupPeriodMillis);
        // For now, reap must come from external
        //timer.scheduleAtFixedRate(new Reaper(), reapInitialDelayMillis, reapPeriodMillis);        
    }
    
    public void initialReap(boolean leaveTrash) {
    	reap(leaveTrash);
    }
    
    private void createMetaNSStore() {
        long            metaNS;

        metaNS = NamespaceUtil.metaInfoNamespace.contextAsLong();
        if (namespaces.get(metaNS) == null) {
            NamespaceStore  metaNSStore;
            
            metaNSStore = new NamespaceStore(metaNS, new File(baseDir, Long.toHexString(metaNS)),
                    NamespaceStore.DirCreationMode.CreateNSDir,
                    NamespaceUtil.metaInfoNamespaceProperties,//spGroup.getRootPolicy(),
                    mgBase, ringMaster, false, activeRetrievals);
            namespaces.put(metaNS, metaNSStore);
        }
    }

    private void addDynamicNamespace(DynamicNamespaceStore nsStore) {
    	dynamicNamespaces.add(nsStore.getNamespace());
        namespaces.put(nsStore.getNamespace(), nsStore);
        // FUTURE - below is a duplicative store since the deeper map also has this
        nsMetaStore.setNamespaceProperties(nsStore.getNamespace(), nsStore.getNamespaceProperties());
        Log.warning(nsStore.getName() +" namespace: ", Long.toHexString(nsStore.getNamespace()));
    }
    
    public void addMemoryObservers(JVMMonitor jvmMonitor) {
        jvmMonitor.addMemoryObserver(nodeNSStore);
    }
    
    public void recoverExistingNamespaces() {
        try {
            File[]  files;
            
            files = baseDir.listFiles();
            if (files != null) {
                List<File>  sortedFiles;
                
                sortedFiles = sortNSDirsForRecovery(files);
                for (File nsDir : sortedFiles) {
                    recoverExistingNamespace(nsDir);
                }
                startLinkWatches();
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
    
    private void startLinkWatches() {
    	for (NamespaceStore nsStore: namespaces.values()) {
            nsStore.startWatches(zk, nsLinkBasePath, this);                	
    	}
    }

    private void recoverExistingNamespace(File nsDir) throws IOException {
        try {
            long    ns;
            NamespaceProperties nsProperties;
            NamespaceStore  parent;
            NamespaceStore	nsStore;
                
            Log.warning("\t\tRecovering: "+ nsDir.getName());
            ns = NumConversion.parseHexStringAsUnsignedLong(nsDir.getName());
            nsProperties = NamespacePropertiesIO.read(nsDir);
            nsMetaStore.setNamespaceProperties(ns, nsProperties);
            if (nsProperties.getParent() != null) {
                long    parentContext;
                
                parentContext = new SimpleNamespaceCreator().createNamespace(nsProperties.getParent()).contextAsLong();
                parent = namespaces.get(parentContext);
                if (parent == null) {
                    throw new RuntimeException("Unexpected parent not found: "+ parentContext);
                }
            } else {
                parent = null;
            }
            nsStore = NamespaceStore.recoverExisting(ns, nsDir, parent, null, mgBase, ringMaster, 
                    activeRetrievals, zk, nsLinkBasePath, this);
            namespaces.put(ns, nsStore);
            nsStore.startWatches(zk, nsLinkBasePath, this);            
            Log.warning("\t\tDone recovering: "+ nsDir.getName());
        } catch (NumberFormatException nfe) {
            nfe.printStackTrace();
            Log.warning("Recovery ignoring unexpected nsDir: ", nsDir);
        }
    }
    
    private List<File> sortNSDirsForRecovery(File[] files) throws IOException {
        List<File>  sorted;
        
        sorted = new ArrayList<>();
        for (File file : files) {
            if (file.isDirectory()) {
                addDirAndParents(sorted, file);
            } else {
                Log.warning("Recovery ignoring: ", file);
            }
        }
        return sorted;
    }

    private void addDirAndParents(List<File> sorted, File childDir) throws IOException {
        if (!sorted.contains(childDir)) {
            NamespaceProperties nsProperties;
            String              parentName;
            
            nsProperties = NamespacePropertiesIO.read(childDir);
            parentName = nsProperties.getParent();
            if (parentName != null) {
                long    parentContext;
                File    parentDir;
                
                parentContext = new SimpleNamespaceCreator().createNamespace(parentName).contextAsLong();
                parentDir = new File(baseDir, Long.toHexString(parentContext));
                addDirAndParents(sorted, parentDir);
            }
            sorted.add(childDir);
        }
    }
    
    public void ensureMetaNamespaceStoreExists() {
        metaNamespaceStore = namespaces.get(NamespaceUtil.metaInfoNamespace.contextAsLong()); 
        if (metaNamespaceStore == null) {
            metaNamespaceStore = getNamespaceStore(NamespaceUtil.metaInfoNamespace.contextAsLong(), NSCreationMode.CreateIfAbsent);
        }
    }
    
    public NamespaceProperties getNamespaceProperties(long ns, NamespaceOptionsRetrievalMode retrievalMode) {
        return nsMetaStore.getNamespaceProperties(ns, retrievalMode);
    }
    
    private NamespaceStore getNamespaceStore(long ns, NSCreationMode mode) {
        NamespaceStore  nsStore;
        
        nsStore = namespaces.get(ns);
        if (nsStore == null && mode == NSCreationMode.CreateIfAbsent) {
            NamespaceStore  old;
            boolean         created;
            NamespaceProperties    nsProperties;

            
            nsProperties = nsMetaStore.getNamespaceProperties(ns, NamespaceOptionsRetrievalMode.FetchRemotely);
            
            created = false;
            // FUTURE - could make this lock finer grained
            LWTThreadUtil.setBlocked();
            nsCreationLock.lock();
            try {
                nsStore = namespaces.get(ns);
                if (nsStore == null) {
                    NamespaceStore  parent;
                    
                    if (nsProperties.getParent() != null) {
                        long    parentNS;
                        
                        parentNS = new SimpleNamespaceCreator().createNamespace(nsProperties.getParent()).contextAsLong();
                        LWTThreadUtil.setNonBlocked();
                        nsCreationLock.unlock();
                        try {
                            parent = getNamespaceStore(parentNS, NSCreationMode.CreateIfAbsent);
                        } finally {
                            LWTThreadUtil.setBlocked();
                            nsCreationLock.lock();
                        }
                        if (parent == null) {
                            throw new RuntimeException("Unexpected parent not created: "+ Long.toHexString(ns));
                        }
                    } else {
                        parent = null;
                    }
                    created = true;
                    nsStore = new NamespaceStore(ns, new File(baseDir, Long.toHexString(ns)),
                                            NamespaceStore.DirCreationMode.CreateNSDir,
                                            nsProperties,//spGroup.getRootPolicy(),
                                            parent,
                                            mgBase, ringMaster, false, activeRetrievals);
                } else {
                    created = false;
                }
            } finally {
                nsCreationLock.unlock();
                LWTThreadUtil.setNonBlocked();
            }
            if (created) {
                old = namespaces.putIfAbsent(ns, nsStore);
                if (old != null) {
                    nsStore = old;
                } else {
                    Log.warning("Created new namespace store: "+ Long.toHexString(ns));
                    nsStore.startWatches(zk, nsLinkBasePath, this);
                }
            }
        }
        return nsStore;
    }
    
    @Override
    public void linkCreated(long child, long parent) {
        NamespaceStore  childNS;
        NamespaceStore  parentNS;
        
        if (debugLink) {
            Log.warning("linkCreated ", String.format("%x %x", child, parent));
        }
        childNS = getNamespaceStore(child, NSCreationMode.CreateIfAbsent);
        parentNS = getNamespaceStore(parent, NSCreationMode.DoNotCreate);
        if (parentNS == null) {
            Log.warning("linkCreated couldn't find parent: "+ Long.toHexString(parent));
            return;
        }
        childNS.linkParent(parentNS);
    }
    
    public OpResult putUpdate(long ns, DHTKey key, long version, byte storageState) {
        NamespaceStore  nsStore;
        
        nsStore = getNamespaceStore(ns, NSCreationMode.DoNotCreate);
        if (nsStore != null) {
            return nsStore.putUpdate_(key, version, storageState);
        } else {
            return OpResult.NO_SUCH_VALUE;
        }
    }
    
    public List<OpResult> putUpdate(long ns, List<? extends DHTKey> updates, long version) {
        NamespaceStore  nsStore;
        
        nsStore = getNamespaceStore(ns, NSCreationMode.DoNotCreate);
        if (nsStore != null) {
            return nsStore.putUpdate(updates, version);
        } else {
            List<OpResult>  results;
            
            results = new ArrayList<>(updates.size());
            for (int i = 0; i < updates.size(); i++) {
                results.add(OpResult.NO_SUCH_VALUE);
            }
            return results;
        }
    }
    
    public void put(long ns, List<StorageValueAndParameters> values, byte[] userData,
                    KeyedOpResultListener resultListener) {
        try {
            NamespaceStore  nsStore;
            
            nsStore = getNamespaceStore(ns, NSCreationMode.CreateIfAbsent);
            nsStore.put(values, userData, resultListener);
        } catch (NamespaceNotCreatedException nnce) {
            for (StorageValueAndParameters value : values) {
                resultListener.sendResult(value.getKey(), OpResult.NO_SUCH_NAMESPACE);
            }
        }
    }
    
    public List<ByteBuffer> retrieve(long ns, List<? extends DHTKey> keys, InternalRetrievalOptions options, UUIDBase opUUID) {
        try {
            NamespaceStore  nsStore;
            
            //System.out.printf("StorageModule.retrieve() %x\n", ns);
            nsStore = getNamespaceStore(ns, NSCreationMode.CreateIfAbsent);
            // FUTURE - Consider using DoNotCreate (below) for get operations. 
            // Can't use DoNotCreate if we have a waitfor.
            //nsStore = getNamespaceStore(ns, NSCreationMode.DoNotCreate);
            if (nsStore != null) {
            	if (retrievalImplementation == RetrievalImplementation.Grouped) {
            		return nsStore.retrieve(keys, options, opUUID);
            	} else {
            		return nsStore.retrieve_nongroupedImpl(keys, options, opUUID);
            	}
            } else {
                return null;
            }
        } catch (NamespaceNotCreatedException nnce) {
            List<ByteBuffer>    results;
            
            results = new ArrayList<>(keys.size());
            for (int i = 0; i < keys.size(); i++) {
                results.add(null);
            }
            return results;
        }
    }
    
    public OpResult snapshot(long ns, long version) {
        NamespaceStore  nsStore;
        
        nsStore = getNamespaceStore(ns, NSCreationMode.CreateIfAbsent);
        return nsStore.snapshot(version);
    }
    
    public void cleanup() {
        for (NamespaceStore ns : namespaces.values()) {
            ns.cleanupPendingWaitFors();
        }
    }
    
    public void reap(boolean leaveTrash) {
    	Stopwatch	sw;
    	
    	Log.warning("Reap");
    	sw = new SimpleStopwatch();
        for (NamespaceStore ns : namespaces.values()) {
        	if (!ns.isDynamic()) {
        		ns.reap(leaveTrash);
        	}
        }
    	sw.stop();
    	Log.warning("Reap complete: "+ sw);
    }
    
    /////////////////////////
    // synchronization code
    
    // ns is Long so that invokeAsync works
    public void getChecksumTreeForLocal(Long ns, UUIDBase uuid, ConvergencePoint targetCP,
                                         ConvergencePoint sourceCP, MessageGroupConnection connection, 
                                         byte[] originator, RingRegion region, IPAndPort replica, Integer timeoutMillis) {
        NamespaceStore  nsStore;
        boolean			success;
        OpResult		result;
        ProtoOpResponseMessageGroup	response;
        
        nsStore = getNamespaceStore(ns, NSCreationMode.CreateIfAbsent);
		success = nsStore.getChecksumTreeForLocal(uuid, targetCP, sourceCP, connection, originator, region, replica, timeoutMillis);
		result = success ? OpResult.SUCCEEDED : OpResult.ERROR;
		response = new ProtoOpResponseMessageGroup(uuid, 0, result, SimpleValueCreator.forLocalProcess().getBytes(), timeoutMillis);
		try {
			connection.sendAsynchronous(response.toMessageGroup(), SystemTimeUtil.systemTimeSource.absTimeMillis() + timeoutMillis);
		} catch (IOException ioe) {
			Log.logErrorWarning(ioe);
		}
    }

    // ns is Long so that invokeAsync works
    public void getChecksumTreeForRemote(Long ns, UUIDBase uuid, ConvergencePoint targetCP,
                                         ConvergencePoint sourceCP, MessageGroupConnection connection, 
                                         byte[] originator, RingRegion region) {
        NamespaceStore  nsStore;
        
        nsStore = getNamespaceStore(ns, NSCreationMode.CreateIfAbsent);
        nsStore.getChecksumTreeForRemote(uuid, targetCP, sourceCP, connection, originator, region);
    }
    
    public void incomingSyncRetrievalResponse(MessageGroup message) {
        NamespaceStore  nsStore;
        
        nsStore = getNamespaceStore(message.getContext(), NSCreationMode.CreateIfAbsent);
        nsStore.incomingSyncRetrievalResponse(message);
    }

    // used only for testing
    /*
    public void requestChecksumTree(long version) {
        for (NamespaceStore nsStore : namespaces.values()) {
            System.out.printf("requestChecksumTree: %x\n", nsStore.getNamespace());
            nsStore.requestChecksumTree(version, OwnerQueryMode.Secondary);
        }
    }
    */
    
    public void incomingChecksumTree(MessageGroup message, MessageGroupConnection connection) {
        NamespaceStore  nsStore;
        ChecksumNode    remoteTree;
        ConvergencePoint    cp;
        
        nsStore = getNamespaceStore(message.getContext(), NSCreationMode.CreateIfAbsent);
        cp = ProtoChecksumTreeMessageGroup.getConvergencePoint(message);
        remoteTree = ProtoChecksumTreeMessageGroup.deserialize(message);
        nsStore.incomingChecksumTree(message.getUUID(), remoteTree, cp, connection);
    }
    
    public static boolean isDynamicNamespace(long context) {
        return dynamicNamespaces.contains(context);
    }
    
    /////////////////////////////////
    
    public void handleNamespaceRequest(MessageGroup message, MessageGroupConnection connection) {
        List<Long>  nsList;
        ProtoNamespaceResponseMessageGroup  protoMG;
        
        if (debugNSRequests) {
            Log.warning("Handling namespace request from: ", connection.getRemoteSocketAddress());
        }
        nsList = ImmutableList.copyOf(namespaces.keySet());
        protoMG = new ProtoNamespaceResponseMessageGroup(message.getUUID(), mgBase.getMyID(), nsList);
        try {
            connection.sendAsynchronous(protoMG.toMessageGroup(), 
                    message.getDeadlineAbsMillis(mgBase.getAbsMillisTimeSource()));
        } catch (IOException ioe) {
            Log.logErrorWarning(ioe);
        }
    }
    
    public void handleNamespaceResponse(MessageGroup message, MessageGroupConnection connection) {
        NamespaceRequest    nsRequest;        
        List<Long>  nsList;
        
        if (debugNSRequests) {
            Log.warning("Received namespace response from: ", connection.getRemoteSocketAddress());
        }
        nsList = ProtoNamespaceResponseMessageGroup.getNamespaces(message);
        for (Long ns : nsList) {
            NamespaceStore  nsStore;
            
            if (debugNSRequests) {
                Log.warning(String.format("ns %x", ns.longValue()));
            }
            nsStore = getNamespaceStore(ns, NSCreationMode.CreateIfAbsent);
            if (debugNSRequests) {
                Log.warning(String.format("ns %x nsStore %x", ns.longValue(), nsStore.getNamespace()));
            }
        }
    }
    
    /////////////////////////////////
    
	public void handleSetConvergenceState(MessageGroup message, MessageGroupConnection connection) {
		ringMaster.setConvergenceState(message, connection);
	}

	public void handleReap(MessageGroup message, MessageGroupConnection connection) {
		OpResult	result;
		ProtoOpResponseMessageGroup	response;
		
        asyncInvocation("reap");
		result = OpResult.SUCCEEDED;
		response = new ProtoOpResponseMessageGroup(message.getUUID(), 0, result, myOriginatorID.getBytes(), message.getDeadlineRelativeMillis());
		try {
			connection.sendAsynchronous(response.toMessageGroup(), SystemTimeUtil.systemTimeSource.absTimeMillis() + message.getDeadlineRelativeMillis());
		} catch (IOException ioe) {
			Log.logErrorWarning(ioe);
		}
	}
	
    /////////////////////////////////
	
    private static final String methodCallPoolName = "StorageMethodCallPool";
    private static final int    methodCallPoolTargetSize = 26;
    public static final int     methodCallPoolMaxSize = 26;
    private static final int    methodCallPoolWorkUnit = 16;
    
    public void asyncInvocation(String methodName, Object... parameters) {
        methodCallWorker.addWork(new MethodCallWork(methodName, parameters));
    }
    
    static class MethodCallWorker extends BaseWorker<MethodCallWork> {
        private final Object    target;
        
        MethodCallWorker(LWTPool methodCallPool, Object target) {
            super(methodCallPool, true, 0);
            this.target = target;
        }
        
        @Override
        public void doWork(MethodCallWork mcw) {
            Method  method;
            
            try {
                method = target.getClass().getMethod(mcw.methodName, parameterTypes(mcw.parameters));
                method.invoke(target, mcw.parameters);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private Class<?>[] parameterTypes(Object[] parameters) {
            Class<?>[] types;
            
            types = new Class[parameters.length];
            for (int i = 0; i < parameters.length; i++) {
                types[i] = parameters[i].getClass();
            }
            return types;
        }
    }
    
    static class MethodCallWork {
        final String    methodName;
        final Object[]  parameters;
        
        MethodCallWork(String methodName, Object[] parameters) {
            this.methodName = methodName;
            this.parameters = parameters;
        }
    }
    
    /////////////////////////////////
    
    class Reaper extends TimerTask {
    	private final boolean	leaveTrash;
    	
        Reaper(boolean leaveTrash) {
        	this.leaveTrash = leaveTrash;
        }
        
        @Override
        public void run() {
            reap(leaveTrash);
        }
    }
    
    class Cleaner extends TimerTask {
        Cleaner() {
        }
        
        @Override
        public void run() {
            cleanup();
        }
    }
}
