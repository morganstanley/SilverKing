package com.ms.silverking.cloud.dht.daemon.storage;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
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
import com.ms.silverking.cloud.dht.daemon.NodeRingMaster2;
import com.ms.silverking.cloud.dht.daemon.RingMapState2;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ChecksumNode;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ConvergencePoint;
import com.ms.silverking.cloud.dht.daemon.storage.management.ManagedNamespaceNotCreatedException;
import com.ms.silverking.cloud.dht.daemon.storage.management.ManagedNamespaceStore;
import com.ms.silverking.cloud.dht.daemon.storage.management.ManagedStorageModule;
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
import com.ms.silverking.code.Constraint;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.numeric.NumUtil;
import com.ms.silverking.thread.lwt.LWTPoolParameters;
import com.ms.silverking.thread.lwt.LWTThreadUtil;
import com.ms.silverking.thread.lwt.asyncmethod.MethodCallWorker;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;
import com.ms.silverking.util.SafeTimerTask;
import com.ms.silverking.util.memory.JVMMonitor;

public class StorageModule implements LinkCreationListener, ManagedStorageModule {
  private final NodeRingMaster2 ringMaster;
  private final ConcurrentMap<Long, NamespaceStore> namespaces;
  private final ConcurrentMap<Long, NamespaceMetricsNamespaceStore> nsMetricsNamespaces;
  private final File baseDir;
  private final NamespaceMetaStore nsMetaStore;
  private MessageGroupBase mgBase;
  private StoragePolicyGroup spGroup;
  private ConcurrentMap<UUIDBase, ActiveProxyRetrieval> activeRetrievals;
  private NodeNamespaceStore nodeNSStore;
  private SystemNamespaceStore systemNSStore;
  private ReplicasNamespaceStore replicasNSStore;
  private Lock nsCreationLock;
  private final ZooKeeperExtended zk;
  private final String nsLinkBasePath;
  private final MethodCallWorker methodCallBlockingWorker;
  private final MethodCallWorker methodCallNonBlockingWorker;
  private final Timer timer;
  private final ValueCreator myOriginatorID;
  private final NodeInfoZK nodeInfoZK;
  private final ReapPolicy reapPolicy;
  private final File trashManualDir;
  private final boolean enableMsgGroupTrace;
  private SafeTimerTask cleanerTask;
  private SafeTimerTask reapTask;
  private JVMMonitor jvmMonitor;

  private NamespaceStore metaNamespaceStore; // used to bootstrap the meta NS store
  // reference held here merely to ensure no GC

  private static final int sessionTimeoutMillis = 5 * 60 * 1000;

  private static final int cleanupPeriodMillis = 5 * 1000;
  private static final int reapMaxInitialDelayMillis = 1 * 60 * 1000;

  //private static final int    primaryConvergencePeriodMillis = 60 * 1000;
  //private static final int    secondaryConvergencePeriodMillis = 60 * 1000;
  private static final int primaryConvergencePeriodMillis = 20 * 60 * 1000;
  private static final int secondaryConvergencePeriodMillis = 5 * 60 * 1000;
  private static final int convergenceStartupDelayMillis = 30 * 1000;
  //private static final int    convergencePeriodMillis = 10 * 60 * 1000;
  //private static final int    convergenceStartupDelayMillis = 1 * 60 * 1000;
  // FUTURE - add notion of idle DHT and converge more aggressively during idle periods
  //          and far less aggressively during busy periods

  private static final boolean debugLink = false;
  private static final boolean debugNSRequests = true;

  // FUTURE - this version evolved from a proof-of-concept implementation
  // could consider a revamped approach in the future, or could continue
  // to evolve this implementation to improve performance and features.
  // see Persistence.doc
  // think about renaming ValueStore if we don't have a different
  // ValueStore class after implementing all of Persistence.doc ideas

  private enum NSCreationMode {CreateIfAbsent, DoNotCreate}

  ;

  public enum RetrievalImplementation {Ungrouped, Grouped}

  ;

  private static final Set<Long> dynamicNamespaces = new ConcurrentSkipListSet<>();
  private static final Set<Long> baseNamespaces = new ConcurrentSkipListSet<>();

  private static final String trashManualDirName = "trash_manual";
  private static final String deletedDirName = "_deleted_";
  private static final String dateFormatStr = "yyyy-mm-dd_hh-mm-ss";
  private static final String utcZone = "UTC";

  private static final RetrievalImplementation retrievalImplementation;

  private static final String metricsObserversProperty = StorageModule.class.getCanonicalName() + ".MetricsObservers";

  static {
    retrievalImplementation = RetrievalImplementation.valueOf(
        PropertiesHelper.systemHelper.getString(DHTConstants.retrievalImplementationProperty,
            DHTConstants.defaultRetrievalImplementation.toString()));
    Log.warningf("retrievalImplementation: %s", retrievalImplementation);
  }

  public StorageModule(NodeRingMaster2 ringMaster, String dhtName, Timer timer, ZooKeeperConfig zkConfig,
      NodeInfoZK nodeInfoZK, ReapPolicy reapPolicy, JVMMonitor jvmMonitor, boolean enableMsgGroupTrace) {
    ClientDHTConfiguration clientDHTConfiguration;

    Constraint.ensureNotNull(ringMaster);
    Constraint.ensureNotNull(dhtName);
    Constraint.ensureNotNull(timer);
    Constraint.ensureNotNull(zkConfig);
    Constraint.ensureNotNull(nodeInfoZK);
    Constraint.ensureNotNull(reapPolicy);
    Constraint.ensureNotNull(jvmMonitor);

    this.enableMsgGroupTrace = enableMsgGroupTrace;
    baseNamespaces.add(NamespaceUtil.metaInfoNamespace.contextAsLong());

    this.timer = timer;
    this.ringMaster = ringMaster;
    this.nodeInfoZK = nodeInfoZK;
    this.reapPolicy = reapPolicy;
    this.jvmMonitor = jvmMonitor;
    ringMaster.setStorageModule(this);
    namespaces = new ConcurrentHashMap<>();
    nsMetricsNamespaces = new ConcurrentHashMap<>();
    baseDir = new File(nodeInfoZK.getDHTNodeConfiguration().getDataBasePath(), dhtName);
    //        baseDir = new File(DHTNodeConfiguration.dataBasePath);    // replace above with this to get rid of
    //        double directory name in path
    this.trashManualDir = new File(baseDir, trashManualDirName);
    clientDHTConfiguration = new ClientDHTConfiguration(dhtName, zkConfig);
    nsMetaStore = NamespaceMetaStore.create(clientDHTConfiguration);
    //spGroup = createTestPolicy();
    spGroup = null;
    myOriginatorID = SimpleValueCreator.forLocalProcess();
    // FUTURE re-enable periodic convergence
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
    methodCallNonBlockingWorker = new MethodCallWorker(LWTPoolParameters.create(methodCallNonBlockingPoolName).maxSize(
        methodCallNonBlockingPoolMaxSize).targetSize(methodCallNonBlockingPoolTargetSize).workUnit(
        methodCallPoolWorkUnit).commonQueue(true), this);
    methodCallBlockingWorker = new MethodCallWorker(LWTPoolParameters.create(methodCallBlockingPoolName).maxSize(
        methodCallBlockingPoolMaxSize).targetSize(methodCallBlockingPoolTargetSize).workUnit(
        methodCallPoolWorkUnit).commonQueue(true), this);
    Log.warning("methodCallPools created");

    nsCreationLock = new ReentrantLock();

    try {
      zk = new ZooKeeperExtended(zkConfig, sessionTimeoutMillis, null);
      nsLinkBasePath = MetaPaths.getInstanceNSLinkPath(dhtName);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public final boolean getEnableMsgGroupTrace() {
    return enableMsgGroupTrace;
  }

  @Override
  public File getBaseDir() {
    return baseDir;
  }

  public void setMessageGroupBase(MessageGroupBase mgBase) {
    this.mgBase = mgBase;
  }

  public void setActiveRetrievals(ConcurrentMap<UUIDBase, ActiveProxyRetrieval> activeRetrievals) {
    this.activeRetrievals = activeRetrievals;
  }

  public void setReady() {
    createMetaNSStore();

    nodeNSStore = new NodeNamespaceStore(mgBase, ringMaster, activeRetrievals, namespaces);
    addBaseDynamicNamespace(nodeNSStore);
    jvmMonitor.addMemoryObserver(nodeNSStore);

    replicasNSStore = new ReplicasNamespaceStore(mgBase, ringMaster, activeRetrievals);
    addBaseDynamicNamespace(replicasNSStore);

    systemNSStore = new SystemNamespaceStore(mgBase, ringMaster, activeRetrievals, namespaces.values(), nodeInfoZK);
    addBaseDynamicNamespace(systemNSStore);

    initializeMetricsObservers(ImmutableList.of(nodeNSStore, systemNSStore));

    this.cleanerTask = new SafeTimerTask(new Cleaner());
    timer.scheduleAtFixedRate(cleanerTask, cleanupPeriodMillis, cleanupPeriodMillis);
    if (reapPolicy.supportsLiveReap()) {
      this.reapTask = new SafeTimerTask(new Reaper());
      timer.scheduleAtFixedRate(reapTask, reapPolicy.getReapIntervalMillis(), reapPolicy.getReapIntervalMillis());
    }
  }

  private void initializeMetricsObservers(List<MetricsNamespaceStore> metricsNamespaceStores) {
    String metricsObservers;

    metricsObservers = PropertiesHelper.systemHelper.getString(metricsObserversProperty,
        UndefinedAction.ZeroOnUndefined);
    if (metricsObservers != null) {
      for (String metricsObserverName : metricsObserversProperty.split(",")) {
        StorageMetricsObserver metricsObserver;

        try {
          metricsObserver = (StorageMetricsObserver) Class.forName(metricsObserverName).newInstance();
          metricsObserver.initialize(metricsNamespaceStores);
          Log.info("Initialized StorageMetricsObserver: " + metricsObserverName);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
          Log.logErrorWarning(e, "Unable to instantiate StorageMetricsObserver: " + metricsObserverName);
        }
      }
    }
  }

  public void stop() {
    cleanerTask.cancel();
    reapTask.cancel();

    methodCallNonBlockingWorker.stopLWTPool();
    methodCallBlockingWorker.stopLWTPool();
    if (nsMetaStore != null) {
      nsMetaStore.stop();
    }
    namespaces.clear();
    zk.close();

  }

  private void createMetaNSStore() {
    long metaNS;

    metaNS = NamespaceUtil.metaInfoNamespace.contextAsLong();
    if (namespaces.get(metaNS) == null) {
      NamespaceStore metaNSStore;

      metaNSStore = new NamespaceStore(metaNS, new File(baseDir, NamespaceUtil.contextToDirName(metaNS)),
          NamespaceStore.DirCreationMode.CreateNSDir, NamespaceUtil.metaInfoNamespaceProperties,
          //spGroup.getRootPolicy(),
          mgBase, ringMaster, false, activeRetrievals);
      namespaces.put(metaNS, metaNSStore);
    }
  }

  private void addDynamicNamespace(DynamicNamespaceStore nsStore) {
    dynamicNamespaces.add(nsStore.getNamespace());
    namespaces.put(nsStore.getNamespace(), nsStore);
    // FUTURE - below is a duplicative store since the deeper map also has this
    // NOTE: We have a memory cache for nsProperties here; This cache needs to be properly updated if we support
    // on-the-fly nsProperties modify
    nsMetaStore.setNamespaceProperties(nsStore.getNamespace(), nsStore.getNamespaceProperties());
    Log.warning(nsStore.getName() + " namespace: ", NamespaceUtil.contextToDirName(nsStore.getNamespace()));
  }

  private void addBaseDynamicNamespace(DynamicNamespaceStore nsStore) {
    baseNamespaces.add(nsStore.getNamespace());
    addDynamicNamespace(nsStore);
  }

  private void addNSMetricsNamespaceForNS(NamespaceStore nsStore) {
    NamespaceMetricsNamespaceStore nsMetricsNamespaceStore;

    Log.warningf("addNSMetricsNamespaceForNS %x", nsStore.getNamespace());
    nsMetricsNamespaceStore = new NamespaceMetricsNamespaceStore(mgBase, ringMaster, activeRetrievals, nsStore);
    nsMetricsNamespaces.put(nsMetricsNamespaceStore.getNamespace(), nsMetricsNamespaceStore);
    //try {
    Log.warningf("nsMetricsNamespaceStore.getName() %s", nsMetricsNamespaceStore.getName());
    //nsMetaStore.getNamespaceOptionsClientCS().createNamespace(nsMetricsNamespaceStore.getName(),
    // nsMetricsNamespaceStore.getNamespaceProperties());
    addDynamicNamespace(nsMetricsNamespaceStore);
    //} catch (NamespaceCreationException e) {
    //    Log.logErrorWarning(e, "Unable to create NamespaceMetricsNamespaceStore for "+ nsStore.getNamespace());
    //}
  }

  public void recoverExistingNamespaces() {
    try {
      File[] files;

      files = baseDir.listFiles();
      if (files != null) {
        List<File> sortedFiles;

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
    for (NamespaceStore nsStore : namespaces.values()) {
      nsStore.startWatches(zk, nsLinkBasePath, this);
    }
  }

  private File renameDeletedNs(File nsDir) throws IOException {
    File destFile;
    String originalName;
    DateFormat dateFormat;

    dateFormat = new SimpleDateFormat(dateFormatStr);
    dateFormat.setTimeZone(TimeZone.getTimeZone(utcZone));
    originalName = nsDir.getName();
    do {
      destFile = new File(trashManualDir, originalName + deletedDirName + dateFormat.format(new Date()));
    } while (destFile.exists());

    Files.move(nsDir.toPath(), destFile.toPath(), ATOMIC_MOVE);
    return destFile;
  }

  private File cleanDeletedNamespace(File nsDir) throws IOException {
    if (trashManualDir.exists()) {
      return renameDeletedNs(nsDir);
    } else {
      if (!trashManualDir.mkdirs() && !trashManualDir.exists()) {
        throw new IOException("Unable to mkdirs: " + trashManualDir);
      } else {
        return renameDeletedNs(nsDir);
      }
    }
  }

  private void recoverExistingNamespace(File nsDir) throws IOException {
    try {
      long ns;
      NamespaceProperties nsProperties;
      NamespaceStore parent;
      NamespaceStore nsStore;

      Log.warning("\t\tRecovering: " + nsDir.getName());
      ns = NamespaceUtil.dirNameToContext(nsDir.getName());
      nsProperties = nsMetaStore.getNsPropertiesForRecovery(nsDir);
      if (nsMetaStore.isAutoDeleteEnabled() && nsProperties == null) { // This namespace is already deleted
        Log.warning("\t\tFind pending-deleted namespace [" + nsDir + "] start moving it to [" + trashManualDir + "]");
        File dest = cleanDeletedNamespace(nsDir);
        Log.warning("\t\tDone move [" + nsDir + "] as [" + dest + "]");
      } else {
        // NOTE: We have a memory cache for nsProperties here; This cache needs to be properly updated if we support
        // on-the-fly nsProperties modify
        nsMetaStore.setNamespaceProperties(ns, nsProperties);
        if (nsProperties.getParent() != null) {
          long parentContext;

          parentContext = new SimpleNamespaceCreator().createNamespace(nsProperties.getParent()).contextAsLong();
          parent = namespaces.get(parentContext);
          if (parent == null) {
            throw new RuntimeException("Unexpected parent not found: " + parentContext);
          }
        } else {
          parent = null;
        }
        nsStore = NamespaceStore.recoverExisting(ns, nsDir, parent, null, mgBase, ringMaster, activeRetrievals, zk,
            nsLinkBasePath, this, reapPolicy, nsProperties);
        namespaces.put(ns, nsStore);
        addNSMetricsNamespaceForNS(nsStore);
        nsStore.startWatches(zk, nsLinkBasePath, this);
        Log.warning("\t\tDone recovering: " + nsDir.getName());
      }
    } catch (NumberFormatException nfe) {
      nfe.printStackTrace();
      Log.warning("Recovery ignoring unexpected nsDir: ", nsDir);
    }
  }

  private List<File> sortNSDirsForRecovery(File[] files) throws IOException {
    List<File> sorted;

    sorted = new ArrayList<>();
    for (File file : files) {
      if (file.isDirectory() && !file.getName().equals(trashManualDirName)) {
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
      String parentName;

      nsProperties = nsMetaStore.getNsPropertiesForRecovery(childDir);
      parentName = nsProperties.getParent();
      if (parentName != null) {
        long parentContext;
        File parentDir;

        parentContext = new SimpleNamespaceCreator().createNamespace(parentName).contextAsLong();
        parentDir = new File(baseDir, NamespaceUtil.contextToDirName(parentContext));
        addDirAndParents(sorted, parentDir);
      }
      sorted.add(childDir);
    }
  }

  @Override
  public ManagedNamespaceStore getManagedNamespaceStore(String nsName) throws ManagedNamespaceNotCreatedException {
    try {
      return getNamespaceStore(NamespaceUtil.nameToContext(nsName), NSCreationMode.CreateIfAbsent);
    } catch (NamespaceNotCreatedException nnce) {
      throw new ManagedNamespaceNotCreatedException(nnce);
    }
  }

  public void ensureMetaNamespaceStoreExists() {
    metaNamespaceStore = namespaces.get(NamespaceUtil.metaInfoNamespace.contextAsLong());
    if (metaNamespaceStore == null) {
      metaNamespaceStore = getNamespaceStore(NamespaceUtil.metaInfoNamespace.contextAsLong(),
          NSCreationMode.CreateIfAbsent);
    }
  }

  public NamespaceProperties getNamespaceProperties(long ns, NamespaceOptionsRetrievalMode retrievalMode) {
    return nsMetaStore.getNamespaceProperties(ns, retrievalMode);
  }

  private NamespaceStore getNamespaceStore(long ns, NSCreationMode mode) {
    NamespaceStore nsStore;
    File nsDir;

    nsDir = new File(baseDir, NamespaceUtil.contextToDirName(ns));
    nsStore = namespaces.get(ns);
    if (nsStore == null && mode == NSCreationMode.CreateIfAbsent) {
      NamespaceStore old;
      boolean created;
      NamespaceProperties nsProperties;

      nsProperties = nsMetaStore.getNamespaceProperties(ns, NamespaceOptionsRetrievalMode.FetchRemotely);

      created = false;
      // FUTURE - could make this lock finer grained
      LWTThreadUtil.setBlocked();
      nsCreationLock.lock();
      try {
        nsStore = namespaces.get(ns);
        if (nsStore == null) {
          NamespaceStore parent;

          if (nsProperties.getParent() != null) {
            long parentNS;

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
              throw new RuntimeException("Unexpected parent not created: " + NamespaceUtil.contextToDirName(ns));
            }
          } else {
            parent = null;
          }
          nsStore = new NamespaceStore(ns, nsDir, nsMetaStore.needPropertiesFileBootstrap() ?
              NamespaceStore.DirCreationMode.CreateNSDir :
              NamespaceStore.DirCreationMode.CreateNSDirNoPropertiesFileBootstrap, nsProperties,
              //spGroup.getRootPolicy(),
              parent, mgBase, ringMaster, false, activeRetrievals, reapPolicy);
          if (!baseNamespaces.contains(nsStore.getNamespace())) {
            addNSMetricsNamespaceForNS(nsStore);
          }
          created = true;
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
          Log.warning("Created new namespace store: " + NamespaceUtil.contextToDirName(ns));
          nsStore.startWatches(zk, nsLinkBasePath, this);
        }
      }
    }
    return nsStore;
  }

  @Override
  public void linkCreated(long child, long parent) {
    NamespaceStore childNS;
    NamespaceStore parentNS;

    if (debugLink) {
      Log.warning("linkCreated ", String.format("%x %x", child, parent));
    }
    childNS = getNamespaceStore(child, NSCreationMode.CreateIfAbsent);
    parentNS = getNamespaceStore(parent, NSCreationMode.DoNotCreate);
    if (parentNS == null) {
      Log.warning("linkCreated couldn't find parent: " + NamespaceUtil.contextToDirName(parent));
      return;
    }
    childNS.linkParent(parentNS);
  }

  public OpResult putUpdate(long ns, DHTKey key, long version, byte storageState) {
    NamespaceStore nsStore;

    nsStore = getNamespaceStore(ns, NSCreationMode.DoNotCreate);
    if (nsStore != null) {
      return nsStore.putUpdate_(key, version, storageState);
    } else {
      return OpResult.NO_SUCH_VALUE;
    }
  }

  public List<OpResult> putUpdate(long ns, List<? extends DHTKey> updates, long version) {
    NamespaceStore nsStore;

    nsStore = getNamespaceStore(ns, NSCreationMode.DoNotCreate);
    if (nsStore != null) {
      return nsStore.putUpdate(updates, version);
    } else {
      List<OpResult> results;

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
      NamespaceStore nsStore;

      nsStore = getNamespaceStore(ns, NSCreationMode.CreateIfAbsent);
      nsStore.put(values, userData, resultListener);
    } catch (NamespaceNotCreatedException nnce) {
      for (StorageValueAndParameters value : values) {
        resultListener.sendResult(value.getKey(), OpResult.NO_SUCH_NAMESPACE);
      }
    }
  }

  public List<ByteBuffer> retrieve(long ns, List<? extends DHTKey> keys, InternalRetrievalOptions options,
      UUIDBase opUUID) {
    try {
      NamespaceStore nsStore;

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
      List<ByteBuffer> results;

      results = new ArrayList<>(keys.size());
      for (int i = 0; i < keys.size(); i++) {
        results.add(null);
      }
      return results;
    }
  }

  public OpResult snapshot(long ns, long version) {
    NamespaceStore nsStore;

    nsStore = getNamespaceStore(ns, NSCreationMode.CreateIfAbsent);
    return nsStore.snapshot(version);
  }

  public void cleanup() {
    for (NamespaceStore ns : namespaces.values()) {
      ns.cleanupPendingWaitFors();
    }
  }

  public void startupReap() {
    Stopwatch sw;

    Log.warning("Startup reap");
    sw = new SimpleStopwatch();
    for (NamespaceStore ns : namespaces.values()) {
      if (!ns.isDynamic()) {
        ns.startupReap();
      }
    }
    sw.stop();
    Log.warningf("Startup reap complete: %f", sw.getElapsedSeconds());
  }

  public void liveReap() {
    if (!RingMapState2.localNodeIsExcluded()) {
      Stopwatch sw;

      if (reapPolicy.verboseReap()) {
        Log.warningAsync("Live reap");
      }
      sw = new SimpleStopwatch();
      for (NamespaceStore ns : namespaces.values()) {
        if (!ns.isDynamic()) {
          ns.liveReap();
        }
      }
      sw.stop();
      if (reapPolicy.verboseReap()) {
        Log.warningAsyncf("Live reap complete: %f", sw.getElapsedSeconds());
      }
    } else {
      if (reapPolicy.verboseReap()) {
        Log.warningAsync("Skipping live reap. Local node is excluded.");
      }
    }
  }

  /////////////////////////
  // synchronization code

  // ns is Long so that invokeAsync works
  public void getChecksumTreeForLocal(Long ns, UUIDBase uuid, ConvergencePoint targetCP, ConvergencePoint sourceCP,
      MessageGroupConnection connection, byte[] originator, RingRegion region, IPAndPort replica,
      Integer timeoutMillis) {
    NamespaceStore nsStore;
    boolean success;
    OpResult result;
    ProtoOpResponseMessageGroup response;

    nsStore = getNamespaceStore(ns, NSCreationMode.CreateIfAbsent);
    success = nsStore.getChecksumTreeForLocal(uuid, targetCP, sourceCP, connection, originator, region, replica,
        timeoutMillis);
    result = success ? OpResult.SUCCEEDED : OpResult.ERROR;
    response = new ProtoOpResponseMessageGroup(uuid, 0, result, SimpleValueCreator.forLocalProcess().getBytes(),
        timeoutMillis);
    try {
      connection.sendAsynchronous(response.toMessageGroup(),
          SystemTimeUtil.skSystemTimeSource.absTimeMillis() + timeoutMillis);
    } catch (IOException ioe) {
      Log.logErrorWarning(ioe);
    }
  }

  // ns is Long so that invokeAsync works
  public void getChecksumTreeForRemote(Long ns, UUIDBase uuid, ConvergencePoint targetCP, ConvergencePoint sourceCP,
      MessageGroupConnection connection, byte[] originator, RingRegion region) {
    NamespaceStore nsStore;

    nsStore = getNamespaceStore(ns, NSCreationMode.CreateIfAbsent);
    nsStore.getChecksumTreeForRemote(uuid, targetCP, sourceCP, connection, originator, region);
  }

  public void incomingSyncRetrievalResponse(MessageGroup message) {
    NamespaceStore nsStore;

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
    NamespaceStore nsStore;
    ChecksumNode remoteTree;
    ConvergencePoint cp;

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
    Set<Long> nsSet;
    List<Long> nsList;
    ProtoNamespaceResponseMessageGroup protoMG;

    if (debugNSRequests) {
      Log.warning("Handling namespace request from: ", connection.getRemoteSocketAddress());
    }
    nsSet = new HashSet<>();
    for (Map.Entry<Long, NamespaceStore> entry : namespaces.entrySet()) {
      if (!entry.getValue().isDynamic()) {
        nsSet.add(entry.getKey());
      }
    }
    nsList = ImmutableList.copyOf(nsSet);
    if (debugNSRequests) {
      Log.warningf("Returning namespaces %s: ", CollectionUtil.toString(nsList));
    }
    protoMG = new ProtoNamespaceResponseMessageGroup(message.getUUID(), mgBase.getMyID(), nsList);
    try {
      connection.sendAsynchronous(protoMG.toMessageGroup(),
          message.getDeadlineAbsMillis(mgBase.getAbsMillisTimeSource()));
    } catch (IOException ioe) {
      Log.logErrorWarning(ioe);
    }
  }

  public void handleNamespaceResponse(MessageGroup message, MessageGroupConnection connection) {
    NamespaceRequest nsRequest;
    List<Long> nsList;

    if (debugNSRequests) {
      Log.warning("Received namespace response from: ", connection.getRemoteSocketAddress());
    }
    nsList = ProtoNamespaceResponseMessageGroup.getNamespaces(message);
    for (Long ns : nsList) {
      NamespaceStore nsStore;

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
    OpResult result;
    ProtoOpResponseMessageGroup response;

    asyncInvocationNonBlocking("reap");
    result = OpResult.SUCCEEDED;
    response = new ProtoOpResponseMessageGroup(message.getUUID(), 0, result, myOriginatorID.getBytes(),
        message.getDeadlineRelativeMillis());
    try {
      connection.sendAsynchronous(response.toMessageGroup(),
          SystemTimeUtil.skSystemTimeSource.absTimeMillis() + message.getDeadlineRelativeMillis());
    } catch (IOException ioe) {
      Log.logErrorWarning(ioe);
    }
  }

  /////////////////////////////////

  private static final String methodCallBlockingPoolName = "StorageBlockingMethodCallPool";
  private static final String methodCallNonBlockingPoolName = "StorageNonBlockingMethodCallPool";
  private static final int _methodCallPoolSize = NumUtil.bound(20, 2, Runtime.getRuntime().availableProcessors() / 2);
  private static final int methodCallBlockingPoolTargetSize = _methodCallPoolSize;
  private static final int methodCallBlockingPoolMaxSize = _methodCallPoolSize;
  private static final int methodCallNonBlockingPoolTargetSize = _methodCallPoolSize;
  private static final int methodCallNonBlockingPoolMaxSize = _methodCallPoolSize;
  private static final int methodCallPoolWorkUnit = 16;

  static {
    Log.warningf("_methodCallPoolSize: %d", _methodCallPoolSize);
  }

  // methods called from here may block on a local or remote async invocation
  public void asyncInvocationBlocking(String methodName, Object... parameters) {
    methodCallBlockingWorker.asyncInvocation(methodName, parameters);
  }

  // methods called from here must not block on another async invocation on this node or any remote node
  public void asyncInvocationNonBlocking(String methodName, Object... parameters) {
    methodCallNonBlockingWorker.asyncInvocation(methodName, parameters);
  }

  /////////////////////////////////

  class Reaper extends TimerTask {
    Reaper() {
    }

    @Override
    public void run() {
      liveReap();
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
