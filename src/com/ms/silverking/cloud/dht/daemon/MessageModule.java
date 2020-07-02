package com.ms.silverking.cloud.dht.daemon;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.SecondaryTarget;
import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.EnumValues;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.common.NamespaceMetaStore.NamespaceOptionsRetrievalMode;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceNotCreatedException;
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceStore;
import com.ms.silverking.cloud.dht.daemon.storage.StorageModule;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ConvergenceController2;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ConvergencePoint;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.LooseConsistency;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.RetrievalProtocol;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.SingleWriterConsistent;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.StorageProtocol;
import com.ms.silverking.cloud.dht.meta.MetaClient;
import com.ms.silverking.cloud.dht.net.ForwardingMode;
import com.ms.silverking.cloud.dht.net.IPAliasMap;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.net.MessageGroupConnection;
import com.ms.silverking.cloud.dht.net.MessageGroupKeyEntry;
import com.ms.silverking.cloud.dht.net.MessageGroupReceiver;
import com.ms.silverking.cloud.dht.net.ProtoChecksumTreeRequestMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoKeyedMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoNopMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoOpResponseMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoPingAckMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoPingMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoPutMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoPutResponseMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoPutUpdateMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoRetrievalMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoSnapshotMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoVersionedBasicOpMessageGroup;
import com.ms.silverking.cloud.dht.net.PutResult;
import com.ms.silverking.cloud.dht.trace.TraceIDProvider;
import com.ms.silverking.cloud.dht.trace.TracerFactory;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.cloud.skfs.dir.DirectoryBase;
import com.ms.silverking.cloud.toporing.PrimarySecondaryIPListPair;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.net.async.AddressStatusProvider;
import com.ms.silverking.net.async.PersistentAsyncServer;
import com.ms.silverking.net.security.AuthResult;
import com.ms.silverking.net.security.Authorizer;
import com.ms.silverking.process.SafeThread;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.thread.lwt.BaseWorker;
import com.ms.silverking.thread.lwt.LWTPool;
import com.ms.silverking.thread.lwt.LWTPoolParameters;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.time.AbsMillisTimeSource;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.SafeTimerTask;
import org.apache.zookeeper.KeeperException;

//import com.ms.silverking.cloud.dht.gcmd.GlobalCommandServer;
//import com.ms.silverking.cloud.dht.net.ProtoGlobalCommandMessageGroup;
//import com.ms.silverking.cloud.dht.net.ProtoGlobalCommandResultMessageGroup;
//import com.ms.silverking.cloud.dht.net.ProtoGlobalCommandUpdateMessageGroup;

/**
 * DHTNode message processing module.
 */
public class MessageModule implements MessageGroupReceiver, StorageReplicaProvider {
  private final NodeRingMaster2 ringMaster;
  private final MessageGroupBase mgBase;
  private final IPAliasMap  aliasMap;
  private final StorageModule storage;
  private final AbsMillisTimeSource absMillisTimeSource;
  private final Worker worker;
  private final IPAndPort myIPAndPort;
  private final IPAndPort[] myIPAndPortArray;
  private final PrimarySecondaryIPListPair systemNamespaceReplicaListPair;
  private final List<IPAndPort> systemNamespaceReplicaList;
  private final Set<IPAndPort> systemNamespaceReplicas;
  private final PeerHealthMonitor peerHealthMonitor;
  private final SafeTimerTask cleanerTask;
  private final byte[]  myIPAndPortAsOriginator; // special case for ping messages
  //private final Timer    pingTimer;
  //private final GlobalCommandServer globalCommandServer;

  // Note - removal of operations is done only in bulk
  private final ConcurrentMap<UUIDBase, ActiveProxyPut> activePuts;
  private final ConcurrentMap<UUIDBase, ActiveProxyRetrieval> activeRetrievals;

  private final StorageProtocol consistencyModeToStorageProtocol[];
  private final StorageProtocol localConsistencyModeToStorageProtocol[];
  private final RetrievalProtocol consistencyModeToRetrievalProtocol[];

  private static final boolean debug = PropertiesHelper.systemHelper.getBoolean(
      MessageModule.class.getCanonicalName() + ".debug", false);
  private static final boolean debugReceivedMessages = false || debug;
  private static final boolean debugCleanup = false || debug;
  private static final boolean debugShortTimeMessages = false || debug;
  private static final int shortTimeWarning = 1000;

  private static final int replicaRetryBufferMS = 30;
  private static final int incomingConnectionBacklog = 4096;

  // FUTURE - add an option to set the number of selector controllers, or the
  // manner in which this number is selected
  //private static final int    numSelectorControllers = 1;
  //private static final int    numSelectorControllers = Runtime.getRuntime().availableProcessors() / 2;
  private static final int numSelectorControllers = Runtime.getRuntime().availableProcessors();
  private static final String selectorControllerClass = "MessageModule";

  private static final int cleanupPeriodMillis = 2 * 1000;
  //private static final int    cleanupPeriodMillis = 5 * 1000;
  //private static final int    replicaTimeoutCheckPeriodMillis = 1 * 1000;

  private static final int statsPeriodMillis = 10 * 1000;

  private static final int maxDirectCallDepth = 10;

  private static final byte[] emptyIPAndPort = new byte[IPAddrUtil.IPV4_IP_AND_PORT_BYTES];

  private final AtomicLong _complete = new AtomicLong();
  private final AtomicLong _incomplete = new AtomicLong();
  private final AtomicLong _notFound = new AtomicLong();
  private static final boolean debugCompletion = false;

  //private static final String    pingTimerName = "PingTimer";
  //private static final long    minPingPeriodMillis = 1 * 1000;
  //private static final long    targetPingsPerSecond = 2;
  private static final long interPingDelayMillis = 100;

  public static final String nodePingerThreadName = "NodePinger";
  private Pinger pingerThread;

  private final boolean enableMsgGroupTrace;

  public MessageModule(NodeRingMaster2 ringMaster, StorageModule storage, AbsMillisTimeSource absMillisTimeSource,
      Timer timer, int serverPort, IPAndPort myIPAndPort, MetaClient mc, IPAliasMap aliasMap, boolean enableMsgGroupTrace)
      throws IOException {

    this.enableMsgGroupTrace = enableMsgGroupTrace;
    mgBase = MessageGroupBase.newServerMessageGroupBase(serverPort, myIPAndPort, incomingConnectionBacklog, this,
        absMillisTimeSource, PersistentAsyncServer.defaultNewConnectionTimeoutController, null, Integer.MAX_VALUE,
        numSelectorControllers, selectorControllerClass, ConvergenceController2.mqListener,
        ConvergenceController2.mqUUID, aliasMap);
    this.aliasMap = aliasMap;
    this.ringMaster = ringMaster;
    this.storage = storage;
    this.absMillisTimeSource = absMillisTimeSource;
    LWTPool workerPool = LWTPoolProvider.createPool(LWTPoolParameters.create("MessageModulePool").targetSize(
        workerPoolTargetSize).maxSize(workerPoolMaxSize));
    worker = new Worker(workerPool);
    // FUTURE - could consider using soft maps instead of explicit cleaning
    //activePuts = new MapMaker().softValues().makeMap();
    activePuts = new ConcurrentHashMap<>();
    activeRetrievals = new ConcurrentHashMap<>();

    this.myIPAndPort = myIPAndPort;
    myIPAndPortArray = new IPAndPort[1];
    myIPAndPortArray[0] = myIPAndPort;
    myIPAndPortAsOriginator = new byte[ValueCreator.BYTES];
    System.arraycopy(myIPAndPort.toByteArray(), 0, myIPAndPortAsOriginator, 0, myIPAndPort.toByteArray().length);

    storage.setMessageGroupBase(mgBase);
    storage.setActiveRetrievals(activeRetrievals);
    storage.recoverExistingNamespaces();
    storage.ensureMetaNamespaceStoreExists();
    cleanerTask = new SafeTimerTask(new Cleaner());
    timer.scheduleAtFixedRate(cleanerTask, cleanupPeriodMillis, cleanupPeriodMillis);
    //timer.scheduleAtFixedRate(new StatsWorker(), statsPeriodMillis, statsPeriodMillis);
    //timer.scheduleAtFixedRate(new ReplicaTimeoutChecker(), replicaTimeoutCheckPeriodMillis,
    // replicaTimeoutCheckPeriodMillis);
    systemNamespaceReplicas = ImmutableSet.of(myIPAndPort);
    systemNamespaceReplicaList = ImmutableList.of(myIPAndPort);
    systemNamespaceReplicaListPair = new PrimarySecondaryIPListPair(systemNamespaceReplicaList, IPAndPort.emptyList);

    consistencyModeToStorageProtocol = new StorageProtocol[EnumValues.consistencyProtocol.length];
    consistencyModeToStorageProtocol[ConsistencyProtocol.LOOSE.ordinal()] = new LooseConsistency(this);
    consistencyModeToStorageProtocol[ConsistencyProtocol.TWO_PHASE_COMMIT.ordinal()] = new SingleWriterConsistent(this);

    localConsistencyModeToStorageProtocol = new StorageProtocol[EnumValues.consistencyProtocol.length];
    localConsistencyModeToStorageProtocol[ConsistencyProtocol.LOOSE.ordinal()] = new LooseConsistency(
        new LocalReplicaProvider());
    localConsistencyModeToStorageProtocol[ConsistencyProtocol.TWO_PHASE_COMMIT.ordinal()] = new SingleWriterConsistent(
        new LocalReplicaProvider());

    consistencyModeToRetrievalProtocol = new RetrievalProtocol[consistencyModeToStorageProtocol.length];
    for (int i = 0; i < consistencyModeToRetrievalProtocol.length; i++) {
      consistencyModeToRetrievalProtocol[i] = (RetrievalProtocol) consistencyModeToStorageProtocol[i];
    }
    //pingTimer = new SafeTimer(pingTimerName);
    //globalCommandServer = null;
    try {
      peerHealthMonitor = new PeerHealthMonitor(mc, myIPAndPort, aliasMap);
    } catch (KeeperException ke) {
      throw new RuntimeException("Exception creating PeerHealthMonitor", ke);
    }
    PeerStateWatcher.setPeerHealthMonitor(peerHealthMonitor);
    mgBase.setPeerHealthMonitor(peerHealthMonitor);
    ringMaster.setPeerHealthMonitor(peerHealthMonitor);
    NamespaceStore.setPeerHealthMonitor(peerHealthMonitor);
    DirectoryBase.setPeerHealthMonitor(peerHealthMonitor);
  }

  public final boolean getEnableMsgGroupTrace() {
    return enableMsgGroupTrace;
  }

  public void enable() {
    peerHealthMonitor.initialize();
    mgBase.enable();
  }

  public void start() {
    establishConnections();
    startPinger();

  }

  public void stop() {
    cleanerTask.cancel();
    if (pingerThread != null) {
      pingerThread.stop();
    }
    mgBase.shutdown();
    worker.stopLWTPool();
  }

  private void startPinger() {
        /*
        int        numReplicas;
        long    pingPeriodMillis;
        
        // FUTURE - this could change
        numReplicas = ringMaster.getAllReplicaServers().size();
        pingPeriodMillis = numReplicas * 1000 / targetPingsPerSecond;
        pingPeriodMillis = Math.max(pingPeriodMillis, minPingPeriodMillis);
        pingTimer.scheduleAtFixedRate(new Pinger(), pingPeriodMillis, pingPeriodMillis);
        */
    Log.warning("Starting Pinger");
    pingerThread = new Pinger();
    new SafeThread(pingerThread, nodePingerThreadName, true).start();
  }

  public void setAddressStatusProvider(AddressStatusProvider addressStatusProvider) {
    mgBase.setAddressStatusProvider(addressStatusProvider);
  }

  @Override
  public void receive(MessageGroup message, MessageGroupConnection connection) {
    int maxDirectCallDepth;
    NamespaceProperties nsProperties;

    if (enableMsgGroupTrace) {
      ProtoKeyedMessageGroup.tryGetTraceIDCopy(message).ifPresent(traceID -> {
        TracerFactory.getTracer().onBothReceiveRequest(traceID);
      });
    }

    nsProperties = storage.getNamespaceProperties(message.getContext(), NamespaceOptionsRetrievalMode.LocalCheckOnly);
    if (nsProperties == null) {
      // If we're using a SelectorThread to do this work, we can't allow this thread to block since we
      // don't have any properties
      maxDirectCallDepth = 0;
    } else {
      if (message.getForwardingMode() == ForwardingMode.DO_NOT_FORWARD && nsProperties.getOptions().getNamespaceServerSideCode() != null) {
        // For non-forwarded messages, disallow all potential server side code usage of SelectorThreads
        // We don't want communication to be dependent on server side code operation
        maxDirectCallDepth = 0;
      } else {
        maxDirectCallDepth = Integer.MAX_VALUE;
      }
    }
    worker.addWork(new MessageAndConnection(message,
            createProxyForConnection(connection, message.getDeadlineAbsMillis(absMillisTimeSource), message.getPeer())),
        maxDirectCallDepth, Integer.MAX_VALUE);
  }

  /**
   * Primary message processing routine.
   *
   * @param message
   * @param connection
   */
  private void handleReceive(MessageGroup message, MessageGroupConnectionProxy connection) {
    try {
      if (debugReceivedMessages) {
        Log.warningf("\t*** Received: %s\n%s", message, Thread.currentThread().getName());
        //message.displayForDebug(true);
      }
      if (debugShortTimeMessages) {
        if (message.getDeadlineRelativeMillis() < shortTimeWarning) {
          Log.warning("\t*** Received short time message: ", message);
          //message.displayForDebug(true);
        }
      }
      if (message.getForwardingMode() != ForwardingMode.DO_NOT_FORWARD) {
        if (debugReceivedMessages) {
          Log.warning("Setting message to peer: ", message.getMessageType());
        }
        message.setPeer(true);
      }
      switch (message.getMessageType()) {
      case PUT:
      case PUT_TRACE:
        handlePut(message, connection);
        break;
      case PUT_RESPONSE:
      case PUT_RESPONSE_TRACE:
        handlePutResponse(message, connection);
        break;
      case PUT_UPDATE:
      case PUT_UPDATE_TRACE:
        handlePutUpdate(message, connection);
        break;
      case RETRIEVE:
      case RETRIEVE_TRACE:
        handleRetrieve(message, connection);
        break;
      case RETRIEVE_RESPONSE:
      case RETRIEVE_RESPONSE_TRACE:
        handleRetrieveResponse(message, connection);
        break;
      case SNAPSHOT:
        handleSnapshot(message, connection);
        break;
      case SYNC_REQUEST:
        handleSyncRequest(message, getConnectionForRemote(connection));
        break;
      case CHECKSUM_TREE_REQUEST:
        handleChecksumTreeRequest(message, getConnectionForRemote(connection));
        break;
      case CHECKSUM_TREE:
        handleIncomingChecksumTree(message, getConnectionForRemote(connection));
        break;
      case OP_NOP:
        handleNop(message, connection);
        break;
      case OP_PING:
        handlePing(message, connection);
        break;
      case OP_PING_ACK:
        handlePingAck(message, connection);
        break;
      case NAMESPACE_REQUEST:
        handleNamespaceRequest(message, getConnectionForRemote(connection));
        break;
      case NAMESPACE_RESPONSE:
        handleNamespaceResponse(message, getConnectionForRemote(connection));
        break;
      case SET_CONVERGENCE_STATE:
        handleSetConvergenceState(message, getConnectionForRemote(connection));
        break;
      case REAP:
        handleReap(message, getConnectionForRemote(connection));
                /*
            case GLOBAL_COMMAND_NEW:
                handleGlobalCommandNew(message, getConnectionForRemote(connection));
                break;
            case GLOBAL_COMMAND_UPDATE:
                handleGlobalCommandUpdate(message, getConnectionForRemote(connection));
                break;
            case GLOBAL_COMMAND_RESPONSE:
                handleGlobalCommandResponse(message, getConnectionForRemote(connection));
                break;
                */
      default:
        throw new RuntimeException("type not handled: " + message.getMessageType());
      }
    } catch (RuntimeException re) {
      Throwable t;

      Log.warning("************************************** " + Thread.currentThread().getName());
      t = re;
      while (t != null) {
        Log.logErrorWarning(t);
        t = t.getCause();
        Log.warning("......................................");
      }
      Log.logErrorWarning(re, "MessageModule error processing connection: " + connection.getConnectionID());
    }
  }

  private StorageProtocol getStorageProtocol(NamespaceOptions nsOptions) {
    return consistencyModeToStorageProtocol[nsOptions.getConsistencyProtocol().ordinal()];
  }

  private StorageProtocol getLocalStorageProtocol(NamespaceOptions nsOptions) {
    return localConsistencyModeToStorageProtocol[nsOptions.getConsistencyProtocol().ordinal()];
  }

  private MessageGroupConnection getConnectionForRemote(MessageGroupConnectionProxy connection) {
    return ((MessageGroupConnectionProxyRemote) connection).getConnection();
  }

  private MessageGroupConnectionProxy createProxyForConnection(MessageGroupConnection connection, long deadline,
      boolean peer) {
    if (connection == null || connection.getRemoteIPAndPort().equals(myIPAndPort)) {
      return new MessageGroupConnectionProxyLocal(worker);
    } else {
      if (!peer) {
        return new MessageGroupConnectionProxyRemote(connection);
      } else {
        try {
          return new MessageGroupConnectionProxyRemote(
              mgBase.getConnection(connection.getRemoteIPAndPort().port(myIPAndPort.getPort()), deadline));
        } catch (ConnectException ce) {
          Log.logErrorWarning(ce, "Reverting to incoming connection for outgoing messages for " + connection);
          return new MessageGroupConnectionProxyRemote(connection);
        }
      }
    }
  }

  private void handlePut(MessageGroup message, MessageGroupConnectionProxy connection) {
    NamespaceProperties nsProperties;
    NamespaceOptions nsOptions;

    nsProperties = storage.getNamespaceProperties(message.getContext(), NamespaceOptionsRetrievalMode.FetchRemotely);
    nsOptions = nsProperties.getOptions();
    if (message.getForwardingMode().forwards()) {
      new ActiveProxyPut(message, ProtoPutMessageGroup.getOptionBuffer(message), connection, this,
          getStorageProtocol(nsOptions), message.getDeadlineAbsMillis(absMillisTimeSource), false,
          nsOptions).startOperation();
    } else {
      new ActiveProxyPut(message, ProtoPutMessageGroup.getOptionBuffer(message), connection, this,
          getLocalStorageProtocol(nsOptions), message.getDeadlineAbsMillis(absMillisTimeSource), true,
          nsOptions).startOperation();
    }
  }

  private RetrievalProtocol getRetrievalProtocol(NamespaceProperties nsProperties) {
    NamespaceOptions nsOptions;

    assert nsProperties != null;
    nsOptions = nsProperties.getOptions();
    assert nsOptions.getConsistencyProtocol() != null;
    assert consistencyModeToRetrievalProtocol != null;
    return consistencyModeToRetrievalProtocol[nsOptions.getConsistencyProtocol().ordinal()];
  }

  private void handleRetrieve(MessageGroup message, MessageGroupConnectionProxy connection) {
    ActiveProxyRetrieval retrieval;
    RetrievalProtocol retrievalProtocol;

    if (enableMsgGroupTrace) {
      ProtoKeyedMessageGroup.tryGetTraceIDCopy(message).ifPresent(traceID -> {
        TracerFactory.getTracer().onBothHandleRetrievalRequest(traceID);
      });
    }

    InternalRetrievalOptions retrieveOpts = ProtoRetrievalMessageGroup.getRetrievalOptions(message);
    AuthResult authorization = null;

    if (Authorizer.isEnabled()) {
      byte[] requestedUser = retrieveOpts.getAuthorizationUser();
      Optional<String> authenticatedUser = connection.getAuthenticatedUser();
      authorization = Authorizer.getPlugin().syncAuthorize(authenticatedUser, requestedUser);
      if (authorization.isSuccessful() && retrieveOpts.getAuthorizationUser() == RetrievalOptions.noAuthorizationUser && authenticatedUser.isPresent()) {
        // if no client user was specified and authorization passed, we can use the authenticated user
        // n.b. that a proxy forward will not further change this field as it'll already be set by the time it sees the message
        // either by the client originally, or the upstream node that handled the initial request
        // including the case where we forward to the same node. Hence after this point this field is certain
        // to match the client's requested or authenticated ID and known to be a successful authorization
        String authenticated = authenticatedUser.get();
        retrieveOpts = retrieveOpts.authorizedAs(authenticated.getBytes());
      }
    }

    try {
      NamespaceProperties nsProperties;

      nsProperties = storage.getNamespaceProperties(message.getContext(), NamespaceOptionsRetrievalMode.FetchRemotely);
      retrievalProtocol = getRetrievalProtocol(nsProperties);
    } catch (NamespaceNotCreatedException nnce) {
      // Allows metrics ns retrievals to return not found without requiring
      // the ns creation on servers where no values are stored
      retrievalProtocol = consistencyModeToRetrievalProtocol[ConsistencyProtocol.LOOSE.ordinal()];
    }		

    retrieval = new ActiveProxyRetrieval(message,
        ProtoRetrievalMessageGroup.getOptionBuffer(message), connection, this, storage, retrieveOpts,
        retrievalProtocol, message.getDeadlineAbsMillis(absMillisTimeSource));

    if (authorization != null && !authorization.isSuccessful()) {
      // We might throw in here if the failure action is to do so,
      // in which case the operation will not be started below
      retrieval.handleAuthorizationFailure(authorization);
    }
    retrieval.startOperation();
  }

  /**
   * Process a retrieval response. For client-initiated retrievals, handle in the
   * retrieval protocol. For locally-initiated synchronization, handle in the synchronization code.
   *
   * @param message
   * @param connection
   */
  void handleRetrieveResponse(MessageGroup message, MessageGroupConnectionProxy connection) {
    Log.fine("handleRetrieveResponse");
    if (debug) {
      Log.warning("rr: " + message + " " + connection);
    }
    //if (Arrays.equals(message.getOriginator(), mgBase.getIPAndPort())) {
    //storage.incomingSyncRetrievalResponse(message);
    //} else {
    ActiveProxyRetrieval activeRetrieval;

    activeRetrieval = activeRetrievals.get(message.getUUID());
    if (activeRetrieval != null) {
      OpResult opResult;

      opResult = activeRetrieval.handleRetrievalResponse(message, connection);
      if (opResult.isComplete()) { // FIXME - think about failures
        activeRetrievals.remove(message.getUUID());
        if (debugCompletion) {
          //_complete.incrementAndGet();
        }

        if (enableMsgGroupTrace) {
          ProtoKeyedMessageGroup.tryGetTraceIDCopy(message).ifPresent(traceID -> {
            TracerFactory.getTracer().onProxyHandleRetrievalResultComplete(traceID);
          });
        }
      } else {
        if (debugCompletion) {
          //_incomplete.incrementAndGet();
        }

        if (enableMsgGroupTrace) {
          ProtoKeyedMessageGroup.tryGetTraceIDCopy(message).ifPresent(traceID -> {
            TracerFactory.getTracer().onProxyHandleRetrievalResultIncomplete(traceID);
          });
        }
      }
    } else {
      //storage.incomingSyncRetrievalResponse(message);
      storage.asyncInvocationNonBlocking("incomingSyncRetrievalResponse", message);
                /*
                Log.warning("Couldn't find activeRetrieval for ", message);
                if (debugCompletion) {
                    //_notFound.incrementAndGet();
                }
                */
    }
    //}
  }

  /**
   * Process a put response.
   *
   * @param message
   * @param connection
   */
  private void handlePutResponse(MessageGroup message, MessageGroupConnectionProxy connection) {
    ActiveProxyPut activePut;

    activePut = activePuts.get(message.getUUID());
    if (activePut != null) {
      OpResult opResult;

      opResult = activePut.handlePutResponse(message, connection);
      if (opResult.isComplete()) {
        activePuts.remove(message.getUUID());
        if (debugCompletion) {
          _complete.incrementAndGet();
        }
      } else {
        if (debugCompletion) {
          _incomplete.incrementAndGet();
        }
      }
    } else {
      Log.infoAsync("Couldn't find active put ", message.getUUID() + " " + new IPAndPort(message.getOriginator()));
      if (debugCompletion) {
        _notFound.incrementAndGet();
      }
    }
  }

  /**
   * Process a put update.
   *
   * @param message
   * @param connection
   */
  private void handlePutUpdate(MessageGroup message, MessageGroupConnectionProxy connection) {
    List<PutResult> results;
    long version;
    byte storageState;

    version = ProtoPutUpdateMessageGroup.getPutVersion(message);
    storageState = ProtoPutUpdateMessageGroup.getStorageState(message);
    if (debug) {
      Log.warningAsyncf("handlePutUpdate storageState: %s ", storageState);
    }
    results = new ArrayList<>();
    for (MessageGroupKeyEntry entry : message.getKeyIterator()) {
      OpResult opResult;

      opResult = storage.putUpdate(message.getContext(), entry, version, storageState);
      results.add(new PutResult(entry, opResult));
    }
    sendPutResults(message, version, connection, results, storageState, message.getDeadlineRelativeMillis());
  }

  ////////////////////////////

  // FUTURE make ActiveProxyPut use this after we have it working for PutUpdate
  // and after the below FUTUREs have been resolved
  protected void sendPutResults(MessageGroup message, long version, MessageGroupConnectionProxy connection,
      List<PutResult> results, byte storageState, int deadlineRelativeMillis) {
    byte[] maybeTraceID;

    if (enableMsgGroupTrace) {
      maybeTraceID = ProtoKeyedMessageGroup.unsafeGetTraceIDCopy(message);
    } else {
      maybeTraceID = TraceIDProvider.noTraceID;
    }

    sendPutResults(message.getUUID(), message.getContext(), version, connection, results, storageState,
        deadlineRelativeMillis, maybeTraceID);
  }

  protected void sendPutResults(UUIDBase uuid, long context, long version, MessageGroupConnectionProxy connection,
      List<PutResult> results, byte storageState, int deadlineRelativeMillis, byte[] maybeTraceID) {
    ProtoPutResponseMessageGroup response;

    if (results.size() > 0) {
      response = new ProtoPutResponseMessageGroup(uuid, context, version,
          // FUTURE - does ProtoPutResponseMessageGroup really need version when we're using the uuid now?
          results.size(), mgBase.getMyID(), storageState, deadlineRelativeMillis,
          maybeTraceID); // FUTURE - allow constructor without this?
      if (debug) {
        Log.warningAsyncf("results.size: %d", results.size());
      }
      for (PutResult result : results) {
        if (debug) {
          Log.warningAsync(result);
        }
        response.addResult(result.getKey(), result.getResult());
      }
      try {
        MessageGroup mg;

        mg = response.toMessageGroup();
        if (Log.levelMet(Level.FINE)) {
          Log.warning("sendResults: " + connection.getConnectionID());
          mg.displayForDebug(true);
        }
        connection.sendAsynchronous(mg, mg.getDeadlineAbsMillis(getAbsMillisTimeSource()));
      } catch (IOException ioe) {
        Log.logErrorWarning(ioe);
      }
    }
  }

  ///////////////////////////////////

  private class LocalReplicaProvider implements StorageReplicaProvider {
    LocalReplicaProvider() {
    }

    @Override
    public IPAndPort[] getReplicas(DHTKey key, OwnerQueryMode oqm, RingOwnerQueryOpType ownerQueryOpType) {
      switch (oqm) {
      case Primary:
        return myIPAndPortArray;
      case Secondary:
        return null;
      case All:
        return myIPAndPortArray;
      default:
        throw new RuntimeException("panic");
      }
    }

    @Override
    public boolean isLocal(IPAndPort replica) {
      return false;
    }
  }

  public PrimarySecondaryIPListPair getReplicaListPair(long context, DHTKey key,
      RingOwnerQueryOpType ownerQueryOpType) {
    if (StorageModule.isDynamicNamespace(context)) {
      return systemNamespaceReplicaListPair;
    } else {
      PrimarySecondaryIPListPair replicaListPair;

      if (debug) {
        Log.warningAsyncf("getReplicaListPair: %s ", key);
      }
      // FUTURE - think about improvements
      replicaListPair = ringMaster.getReplicaListPair(key, ownerQueryOpType);
      if (debug) {
        Log.fineAsyncf("%s \t %s :", key, replicaListPair);
      }
      return replicaListPair;
    }
  }

  public List<IPAndPort> getReplicaList(long context, DHTKey key, OwnerQueryMode oqm,
      RingOwnerQueryOpType ownerQueryOpType) {
    if (StorageModule.isDynamicNamespace(context)) {
      return systemNamespaceReplicaList;
    } else {
      List<IPAndPort> replicaList;

      if (debug) {
        Log.warningAsyncf("getReplicas %s \t %s", key, oqm);
      }
      // FUTURE - think about improvements
      replicaList = ringMaster.getReplicaList(key, oqm, ownerQueryOpType);
      if (debug) {
        Log.warningAsyncf("%s \t %s", key, CollectionUtil.toString(replicaList, ':'));
      }
      return replicaList;
    }
  }

  @Override
  public IPAndPort[] getReplicas(DHTKey key, OwnerQueryMode oqm, RingOwnerQueryOpType ownerQueryOpType) {
    IPAndPort[] replicas;

    if (debug) {
      Log.warningAsyncf("getPrimaryReplicas ", key);
    }
    // FUTURE - think about improvements
    replicas = ringMaster.getReplicas(key, oqm, ownerQueryOpType);
    if (debug) {
      Log.warningAsyncf("%s \t %s", key, IPAndPort.arrayToString(replicas));
    }
    return replicas;
  }

  public IPAndPort localIPAndPort() {
    return myIPAndPort;
  }

  @Override
  public boolean isLocal(IPAndPort replica) {
    if (debug) {
      Log.warningAsyncf("#### %s %s\t%s", replica, myIPAndPort, replica.equals(myIPAndPort));
    }
    return replica.equals(myIPAndPort);
  }

  public Set<IPAndPort> getSecondarySet(Set<SecondaryTarget> secondaryTargets) {
    return ringMaster.getSecondarySet(secondaryTargets);
  }

  ///////////////////////////////////

  private void handleSnapshot(MessageGroup message, MessageGroupConnectionProxy connection) {
    long version;
    ProtoOpResponseMessageGroup response;
    OpResult result;

    if (Log.levelMet(Level.FINE)) {
      message.displayForDebug();
    }
    version = ProtoSnapshotMessageGroup.getVersion(message);
    result = storage.snapshot(message.getContext(), version);
    response = new ProtoOpResponseMessageGroup(message.getUUID(), message.getContext(), result, mgBase.getMyID(),
        message.getDeadlineRelativeMillis());
    try {
      connection.sendAsynchronous(response.toMessageGroup(), message.getDeadlineAbsMillis(absMillisTimeSource));
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

  // sync requests are only for testing
  private void handleSyncRequest(MessageGroup message, MessageGroupConnection connection) {
    long version;
    ProtoOpResponseMessageGroup response;

    if (Log.levelMet(Level.FINE)) {
      message.displayForDebug();
    }
    Log.warning("handleSyncRequest");
    version = ProtoVersionedBasicOpMessageGroup.getVersion(message);
    requestChecksumTree(version);
    response = new ProtoOpResponseMessageGroup(message.getUUID(), message.getContext(), OpResult.SUCCEEDED,
        mgBase.getMyID(), message.getDeadlineRelativeMillis());
    try {
      connection.sendAsynchronous(response.toMessageGroup(), message.getDeadlineAbsMillis(absMillisTimeSource));
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

  // used only for testing
  private void requestChecksumTree(long version) {
    Log.warning("requestChecksumTree");
    throw new RuntimeException("deprecated testing"); // FUTURE remove after double checking
    //storage.requestChecksumTree(version);
  }

  private void handleChecksumTreeRequest(MessageGroup message, MessageGroupConnection connection) {
    ConvergencePoint targetCP;
    ConvergencePoint sourceCP;
    RingRegion region;
    boolean localFlag;

    if (Log.levelMet(Level.FINE)) {
      Log.warning("handleChecksumTreeRequest");
      message.displayForDebug();
    }
    targetCP = ProtoChecksumTreeRequestMessageGroup.getTargetConvergencePoint(message);
    sourceCP = ProtoChecksumTreeRequestMessageGroup.getSourceConvergencePoint(message);
    region = ProtoChecksumTreeRequestMessageGroup.getRegion(message);
    localFlag = ProtoChecksumTreeRequestMessageGroup.getLocalFlag(message);
    //storage.getChecksumTreeForRemote(message.getContext(), message.getUUID(),
    //                        targetCP, sourceCP, connection, message.getOriginator(), region);
    if (!localFlag) {
      storage.asyncInvocationNonBlocking("getChecksumTreeForRemote", message.getContext(), message.getUUID(), targetCP,
          sourceCP, connection, message.getOriginator(), region);
    } else {
      IPAndPort replica;

      replica = ProtoChecksumTreeRequestMessageGroup.getReplica(message);
      storage.asyncInvocationBlocking("getChecksumTreeForLocal", message.getContext(), message.getUUID(), targetCP,
          sourceCP, connection, message.getOriginator(), region, replica, message.getDeadlineRelativeMillis());
    }
  }

  private void handleIncomingChecksumTree(MessageGroup message, MessageGroupConnection connection) {
    if (Log.levelMet(Level.FINE)) {
      Log.warning("handleIncomingChecksumTree");
      message.displayForDebug();
    }
    //storage.incomingChecksumTree(message, connection);
    storage.asyncInvocationNonBlocking("incomingChecksumTree", message, connection);
  }

  ////////////////////////////

  private void handleNamespaceRequest(MessageGroup message, MessageGroupConnection connection) {
    storage.handleNamespaceRequest(message, connection);
  }

  private void handleNamespaceResponse(MessageGroup message, MessageGroupConnection connection) {
    //storage.handleNamespaceResponse(message, connection);
    storage.asyncInvocationNonBlocking("handleNamespaceResponse", message, connection);
  }

  private void handleSetConvergenceState(MessageGroup message, MessageGroupConnection connectionForRemote) {
    storage.handleSetConvergenceState(message, connectionForRemote);
  }

  private void handleReap(MessageGroup message, MessageGroupConnection connectionForRemote) {
    storage.handleReap(message, connectionForRemote);
  }

  ////////////////////////////
    
    /*
    private void handleGlobalCommandNew(MessageGroup message, MessageGroupConnection connectionForRemote) {
        globalCommandServer.newGlobalCommand(ProtoGlobalCommandMessageGroup.getGlobalCommand(message), 
                                             ProtoGlobalCommandMessageGroup.getCommandID(message),
                                             connectionForRemote);
    }
    
    private void handleGlobalCommandUpdate(MessageGroup message, MessageGroupConnection connectionForRemote) {
        globalCommandServer.updateGlobalCommand(ProtoGlobalCommandUpdateMessageGroup.getState(message),
                                                ProtoGlobalCommandMessageGroup.getCommandID(message));
    }

    private void handleGlobalCommandResponse(MessageGroup message, MessageGroupConnection connectionForRemote) {
        globalCommandServer.handleCommandResponse(ProtoGlobalCommandResultMessageGroup.getGlobalCommandUpdateResult
        (message),
                ProtoGlobalCommandResultMessageGroup.getCommandID(message));
    }
    */

  ////////////////////////////

  private void handleNop(MessageGroup message, MessageGroupConnectionProxy connection) {
    Log.finef("%s %s", message.getMessageType(), connection.getConnectionID());
  }

  private void handlePing(MessageGroup message, MessageGroupConnectionProxy connection) {
    if (Log.levelMet(Level.FINE)) {
      Log.finef("%s %s %s %s", message.getMessageType(), connection.getConnectionID(), message.getUUID(),
          Thread.currentThread().getName());
    }
    if (connection instanceof MessageGroupConnectionProxyRemote) {
      MessageGroupConnectionProxyRemote c;
      ProtoPingAckMessageGroup ack;
      IPAndPort source;

      // Ensure that the alias map can map from a remote interface back to the daemon IP
      source = new IPAndPort(message.getOriginator());
      aliasMap.addInterfaceToDaemon(connection.getConnection().getRemoteIPAndPort(), source);
      if (ringMaster.getInstanceExclusionSet().contains(source.getIPAsString())) {
        if (pingerThread != null) {
          Log.infof("Requesting ping of excluded daemon: %s", source);
          pingerThread.requestPing(source);
        } else {
          Log.infof("No pinger thread available, not requesting ping of excluded daemon: %s", source);
        }
      }
      // Special case for ping messages - we send ip and port as originator
      ack = new ProtoPingAckMessageGroup(myIPAndPortAsOriginator, message.getUUID());
      c = (MessageGroupConnectionProxyRemote) connection;
      try {
        c.sendAsynchronous(ack.toMessageGroup(), Long.MAX_VALUE);
      } catch (IOException ioe) {
        Log.logErrorWarning(ioe);
      }
    }
  }

  private void handlePingAck(MessageGroup message, MessageGroupConnectionProxy connection) {
    if (Log.levelMet(Level.FINE)) {
      Log.finef("%s %s %s %s", message.getMessageType(), connection.getConnectionID(), message.getUUID(),
          Thread.currentThread().getName());
    }
    if (connection instanceof MessageGroupConnectionProxyRemote) {
      MessageGroupConnectionProxyRemote c;

      c = (MessageGroupConnectionProxyRemote) connection;
      peerHealthMonitor.removeSuspect(c.getConnection().getRemoteIPAndPort());
    }
  }

  private void establishConnections() {
    for (IPAndPort replica : ringMaster.getAllCurrentReplicaServers()) {
      ProtoNopMessageGroup nop;

      nop = new ProtoNopMessageGroup(mgBase.getMyID());
      Log.warning("Priming: ", replica);
      mgBase.send(nop.toMessageGroup(), replica);
    }
  }

  ////////////////////////////

  StorageModule getStorage() {
    return storage;
  }

  AbsMillisTimeSource getAbsMillisTimeSource() {
    return absMillisTimeSource;
  }

  MessageGroupBase getMessageGroupBase() {
    return mgBase;
  }

  void addActivePut(UUIDBase uuid, ActiveProxyPut activeProxyPut) {
    activePuts.put(uuid, activeProxyPut);
  }

  void addActiveRetrieval(UUIDBase uuid, ActiveProxyRetrieval activeProxyRetrieval) {
    activeRetrievals.putIfAbsent(uuid, activeProxyRetrieval);
  }

  ///////////////////////////////////

  @Override
  public String toString() {
    return mgBase.toString();
  }

  ///////////////////////////////////

  static class MessageAndConnection {
    final MessageGroup message;
    final MessageGroupConnectionProxy connection;

    MessageAndConnection(MessageGroup message, MessageGroupConnectionProxy connection) {
      this.message = message;
      this.connection = connection;
    }
  }

  private static final int workerPoolTargetSize = PropertiesHelper.systemHelper.getInt(
      MessageModule.class.getCanonicalName() + ".WorkerPoolTargetSize",
      Math.max(Runtime.getRuntime().availableProcessors() / 2, 2));
  private static final int workerPoolMaxSize = PropertiesHelper.systemHelper.getInt(
      MessageModule.class.getCanonicalName() + ".WorkerPoolMaxSize",
      Math.max(Runtime.getRuntime().availableProcessors() / 2, 2));

  class Worker extends BaseWorker<MessageAndConnection> {
    Worker(LWTPool workerPool) {
      super(workerPool, true, maxDirectCallDepth);
    }

    @Override
    public void doWork(MessageAndConnection m) {
      handleReceive(m.message, m.connection);
    }
  }

  /////////////////////////////////

  // deprecated in favor of one pass combining cleanup and replica timeout checks
    /*
    class ReplicaTimeoutChecker extends TimerTask {
        ReplicaTimeoutChecker() {
        }
        
        @Override
        public void run() {
            checkForTimeouts();
        }
        
        private void checkForTimeouts() {
            long    absTimeMillis;
            
            Log.info("Checking MessageModule maps for timeouts");
            //System.out.println(activePuts.size());
            absTimeMillis = absMillisTimeSource.absTimeMillis();
            checkMap(activePuts, absTimeMillis);
            checkMap(activeRetrievals, absTimeMillis);
            Log.info("Done checking MessageModule maps for timeouts");
        }
        
        private void checkMap(ConcurrentMap<UUIDBase, ? extends ActiveProxyOperation<?, ?>> map, long absTimeMillis) {
            for (Map.Entry<UUIDBase, ? extends ActiveProxyOperation<?, ?>> entry : map.entrySet()) {
                entry.getValue().checkForReplicaTimeouts(absTimeMillis);
            }
        }        
    }
    */

  /**
   * Cleans up complete and timed out operations.
   * Also, sends retries for replica timeouts.
   */
  class Cleaner extends TimerTask {
    Cleaner() {
    }

    @Override
    public void run() {
      cleanup();
    }

    private void cleanup() {
      long absTimeMillis;

      if (debugCompletion) {
        System.out.printf("c %d\ti %d\tn %d\n", _complete.get(), _incomplete.get(), _notFound.get());
      }

      if (debugCleanup) {
        Log.info("Cleaning MessageModule maps");
      }
      //System.out.println(activePuts.size());
      absTimeMillis = absMillisTimeSource.absTimeMillis();
      cleanupMap(activePuts, absTimeMillis);
      cleanupMap(activeRetrievals, absTimeMillis);
      if (debugCleanup) {
        Log.info("Done cleaning MessageModule maps");
      }
    }

    private void cleanupMap(ConcurrentMap<UUIDBase, ? extends ActiveProxyOperation<?, ?>> map, long absTimeMillis) {
      boolean newTimeouts;

      newTimeouts = false;
      for (Map.Entry<UUIDBase, ? extends ActiveProxyOperation<?, ?>> entry : map.entrySet()) {
        if (entry.getValue().hasTimedOut(
            absTimeMillis) || entry.getValue().getOpResult().isComplete()) { // FIXME - think about failures
          if (debugCleanup) {
            System.out.printf("Removing %s\n", entry.getKey());
          }
          map.remove(entry.getKey());
        } else {
          Set<IPAndPort> timedOutReplicas;

          // We don't retry replicas that are near timing out. This is simply
          // to avoid littering logs with sendTimedOut messages. I.e. we could
          // actually do a retry right up until the timeout, but doing so
          // causes some sends to time out and we currently like to log
          // timed out sends since they can indicate deeper trouble.
          // Send timeouts due to sending near the deadline is really not
          // something that we want to see logged.
          timedOutReplicas = entry.getValue().checkForReplicaTimeouts(absTimeMillis - replicaRetryBufferMS);
          for (IPAndPort timedOutReplica : timedOutReplicas) {
            peerHealthMonitor.addSuspect(timedOutReplica, PeerHealthIssue.ReplicaTimeout);
            newTimeouts = true;
          }
        }
      }
      if (newTimeouts) {
        //ringMaster.updateCurMapState(); deprecated method
      }
    }
  }

  class Pinger extends TimerTask {
    private final Set<IPAndPort> pingRequests;
    private boolean running = true;

    Pinger() {
      pingRequests = new ConcurrentSkipListSet<>();
    }

    public void requestPing(IPAndPort replica) {
      pingRequests.add(replica);
    }

    @Override
    public void run() {
      Log.warning("Pinger running");
      while (running) {
        try {
          pingReplicas();
          peerHealthMonitor.refreshZK();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    private void pingReplicas() {
      Set<IPAndPort>  _pingRequests;
      Set<IPAndPort>  pingTargets;

      Log.fine("Pinging replicas");
      _pingRequests = ImmutableSet.copyOf(pingRequests);
      pingRequests.removeAll(_pingRequests);
      pingTargets = new HashSet<>(ringMaster.getAllCurrentAndTargetNonExcludedNonLocalReplicaServers());
      pingTargets.addAll(_pingRequests);
      for (IPAndPort replica : pingTargets) {
        Log.finef("Pinging %s", replica);
        // Special case for ping messages - we send ip and port as originator
        mgBase.send(new ProtoPingMessageGroup(myIPAndPortAsOriginator).toMessageGroup(), replica);
        ThreadUtil.sleep(interPingDelayMillis);
      }
      ThreadUtil.sleep(interPingDelayMillis);
    }

    private void stop() {
      running = false;
    }
  }

  class StatsWorker extends TimerTask {
    StatsWorker() {
    }

    @Override
    public void run() {
      doStats();
    }

    private void doStats() {
      mgBase.writeStats();
    }
  }
}
