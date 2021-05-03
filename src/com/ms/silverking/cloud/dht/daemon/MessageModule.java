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
import com.ms.silverking.cloud.dht.ForwardingMode;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.SecondaryTarget;
import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.client.FailureCause;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.EnumValues;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.common.NamespaceMetaStore.NamespaceOptionsRetrievalMode;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
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
import com.ms.silverking.cloud.dht.net.IPAliasMap;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.net.MessageGroupConnection;
import com.ms.silverking.cloud.dht.net.MessageGroupKeyEntry;
import com.ms.silverking.cloud.dht.net.MessageGroupReceiver;
import com.ms.silverking.cloud.dht.net.ProtoChecksumTreeRequestMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoErrorResponseMessageGroup;
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
import com.ms.silverking.cloud.dht.record.RecorderFactory;
import com.ms.silverking.cloud.dht.trace.TraceIDProvider;
import com.ms.silverking.cloud.dht.trace.TracerFactory;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.cloud.skfs.dir.DirectoryBase;
import com.ms.silverking.cloud.toporing.PrimarySecondaryIPListPair;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.net.async.AddressStatusProvider;
import com.ms.silverking.net.async.PersistentAsyncServer;
import com.ms.silverking.net.security.AuthFailedException;
import com.ms.silverking.net.security.AuthorizationResult;
import com.ms.silverking.net.security.Authorizer;
import com.ms.silverking.process.SafeThread;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.thread.lwt.BaseWorker;
import com.ms.silverking.thread.lwt.LWTPool;
import com.ms.silverking.thread.lwt.LWTPoolParameters;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.thread.lwt.util.Broadcaster;
import com.ms.silverking.thread.lwt.util.Listener;
import com.ms.silverking.time.AbsMillisTimeSource;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.SafeTimerTask;

/**
 * DHTNode message processing module.
 */
public class MessageModule implements MessageGroupReceiver, StorageReplicaProvider {
  private final NodeRingMaster2 ringMaster;
  private final MessageGroupBase mgBase;
  private final IPAliasMap aliasMap;
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
  private final byte[] myIPAndPortAsOriginator; // special case for ping messages
  //private final Timer    pingTimer;
  //private final GlobalCommandServer globalCommandServer;
  private final ExclusionChangeListener exclusionChangeListener;

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
  private final boolean enableMsgGroupRecorder;

  public enum OnSelfExclusion {DoNothing, DisconnectAll}

  private static final OnSelfExclusion onSelfExclusion;

  private static Logger log = LoggerFactory.getLogger(MessageModule.class);
  static {
    String value = PropertiesHelper.systemHelper.getString(DHTConstants.onSelfExclusionProperty,
        OnSelfExclusion.DoNothing.toString());
    log.info("OnSelfExclusion: {}", value);
    onSelfExclusion = OnSelfExclusion.valueOf(value);
  }

  public MessageModule(NodeRingMaster2 ringMaster, StorageModule storage, AbsMillisTimeSource absMillisTimeSource,
      Timer timer, int serverPort, IPAndPort myIPAndPort, MetaClient mc, IPAliasMap aliasMap,
      boolean enableMsgGroupTrace, boolean enableMsgGroupRecorder) throws IOException {

    this.enableMsgGroupTrace = enableMsgGroupTrace;
    this.enableMsgGroupRecorder = enableMsgGroupRecorder;

    mgBase = MessageGroupBase.newServerMessageGroupBase(serverPort, myIPAndPort, incomingConnectionBacklog, this,
        absMillisTimeSource, PersistentAsyncServer.defaultNewConnectionTimeoutController, null, Integer.MAX_VALUE,
        numSelectorControllers, selectorControllerClass, ConvergenceController2.mqListener,
        ConvergenceController2.mqUUID, aliasMap);
    this.aliasMap = aliasMap;
    this.ringMaster = ringMaster;
    log.warn("setting %s SelfExclusionResponder {}", onSelfExclusion);
    if (onSelfExclusion == OnSelfExclusion.DisconnectAll) {
      SelfExclusionResponder responder = new DisconnectAllExclusionResponder(mgBase.getConnectionController());
      ringMaster.setSelfExclusionResponder(responder);
    }
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
    exclusionChangeListener = new ExclusionChangeListener();
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
    log.warn("Starting Pinger");
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
    try {
      if (enableMsgGroupTrace) {
        Optional<byte[]> maybeTraceId = ProtoKeyedMessageGroup.tryGetTraceIDCopy(message);

        maybeTraceId.ifPresent(traceID -> {
          TracerFactory.getTracer().onBothReceiveRequest(traceID, message.getMessageType());
          // Tracing must be enabled so that only client requests are recorded, not proxy->node messages.
          if (enableMsgGroupRecorder) {
            try {
              RecorderFactory.getRecorder().record(message, traceID);
            } catch(Exception ex) {
              log.debug("Request recording failed", ex);
            }
          }
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
              createProxyForConnection(connection, message.getDeadlineAbsMillis(absMillisTimeSource),
                  message.getPeer())),
          maxDirectCallDepth, Integer.MAX_VALUE);
    } catch (Exception e) {
      log.warn("Caught exception when processing message with id {}, attempting to send an error response back",
          message.getUUID());
      try {
        MessageGroupConnection conn = getConnectionForRemote(
            createProxyForConnection(connection, message.getDeadlineAbsMillis(absMillisTimeSource), message.getPeer()));
        ProtoErrorResponseMessageGroup response = new ProtoErrorResponseMessageGroup(message.getUUID(),
            message.getContext(), OpResult.fromFailureCause(FailureCause.ERROR), message.getOriginator(),
            message.getDeadlineRelativeMillis());
        conn.sendAsynchronous(response.toMessageGroup(),
            SystemTimeUtil.skSystemTimeSource.absTimeMillis() + message.getDeadlineRelativeMillis());
      } catch (IOException ioe) {
        log.warn("Error while sending message",ioe);
      }
    }
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
        log.warn("\t*** Received: {}\n{}", message, Thread.currentThread().getName());
      }
      if (debugShortTimeMessages) {
        if (message.getDeadlineRelativeMillis() < shortTimeWarning) {
          log.warn("\t*** Received short time message: {}", message);
        }
      }
      if (message.getForwardingMode() != ForwardingMode.DO_NOT_FORWARD) {
        if (debugReceivedMessages) {
          log.warn("Setting message to peer: {}", message.getMessageType());
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
      case ERROR_RESPONSE:
        handleErrorResponse(message, connection);
      default:
        throw new RuntimeException("type not handled: " + message.getMessageType());
      }
    } catch (RuntimeException re) {
      Throwable t;

      log.warn("************************************** {}", Thread.currentThread().getName());
      t = re;
      while (t != null) {
        log.warn("Error whiles handling receive",t);
        t = t.getCause();
        log.warn("......................................");
      }
      log.warn("MessageModule error processing connection: {}", connection.getConnectionID(), re);
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
        } catch (AuthFailedException | ConnectException e) {
          log.warn("Reverting to incoming connection for outgoing messages for {}", connection, e);
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
    AuthorizationResult authorization = null;

    if (Authorizer.isEnabled()) {
      byte[] requestedUser = retrieveOpts.getAuthorizationUser();
      Optional<String> authenticatedUser = connection.getAuthenticatedUser();
      authorization = Authorizer.getPlugin().syncAuthorize(authenticatedUser, requestedUser);
      if (authorization.isSuccessful() && retrieveOpts.getAuthorizationUser() == RetrievalOptions.noAuthorizationUser && authenticatedUser.isPresent()) {
        // if no client user was specified and authorization passed, we can use the authenticated user
        // n.b. that a proxy forward will not further change this field as it'll already be set by the time it sees
        // the message
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

    retrieval = new ActiveProxyRetrieval(message, ProtoRetrievalMessageGroup.getOptionBuffer(message), connection, this,
        storage, retrieveOpts, retrievalProtocol, message.getDeadlineAbsMillis(absMillisTimeSource));

    if (authorization != null && !authorization.isSuccessful()) {
      boolean continueOperation = false;

      try {
        continueOperation = retrieval.handleAuthorizationFailure(authorization);
      } catch (AuthFailedException afe) {
        // This will be caught and be logged
        throw new RuntimeException(afe);
      } finally {
        if (enableMsgGroupTrace && !continueOperation) {
          ProtoKeyedMessageGroup.tryGetTraceIDCopy(message).ifPresent(traceID -> {
            TracerFactory.getTracer().onAuthorizationFailure(traceID);
          });
        }
      }
      if (continueOperation) {
        retrieval.startOperation();
      }
    } else {
      retrieval.startOperation();
    }
  }

  /**
   * Process a retrieval response. For client-initiated retrievals, handle in the
   * retrieval protocol. For locally-initiated synchronization, handle in the synchronization code.
   *
   * @param message
   * @param connection
   */
  void handleRetrieveResponse(MessageGroup message, MessageGroupConnectionProxy connection) {
    log.debug("handleRetrieveResponse");
    if (debug) {
      log.warn("rr: {} {}", message, connection);
    }

    ActiveProxyRetrieval activeRetrieval;

    activeRetrieval = activeRetrievals.get(message.getUUID());
    if (activeRetrieval != null) {
      OpResult opResult;

      opResult = activeRetrieval.handleRetrievalResponse(message, connection);
      if (opResult.isComplete()) { // FIXME - think about failures
        activeRetrievals.remove(message.getUUID());

        if (enableMsgGroupTrace) {
          ProtoKeyedMessageGroup.tryGetTraceIDCopy(message).ifPresent(traceID -> {
            TracerFactory.getTracer().onProxyHandleRetrievalResultComplete(traceID);
          });
        }
      } else {
        if (enableMsgGroupTrace) {
          ProtoKeyedMessageGroup.tryGetTraceIDCopy(message).ifPresent(traceID -> {
            TracerFactory.getTracer().onProxyHandleRetrievalResultIncomplete(traceID);
          });
        }
      }
    } else {
      storage.asyncInvocationNonBlocking("incomingSyncRetrievalResponse", message);
                }
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
      log.info("Couldn't find active put {}", message.getUUID() + " " + new IPAndPort(message.getOriginator()));
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
        log.warn("results.size: {}", results.size());
      }
      for (PutResult result : results) {
        if (debug) {
          log.warn("{}",result);
        }
        response.addResult(result.getKey(), result.getResult());
      }
      try {
        MessageGroup mg;

        mg = response.toMessageGroup();
        if (log.isDebugEnabled()) {
          log.warn("sendResults: {}", connection.getConnectionID());
          mg.displayForDebug(true);
        }
        connection.sendAsynchronous(mg, mg.getDeadlineAbsMillis(getAbsMillisTimeSource()));
      } catch (IOException ioe) {
        log.warn("Error whiles sending message",ioe);
      }
    }
  }

  protected void handleErrorResponse(MessageGroup message, MessageGroupConnectionProxy connection) {
    log.warn("{} {} {}", message.getUUID(), message.getMessageType(), connection.getConnectionID());
    //Check for active operations (puts or retrieval) corresponding to this UUID and mark the requests as failed
    //This should happen on the proxy, when it receives responses.
    ActiveProxyOperation op = null;
    if (activePuts.containsKey(message.getUUID())) {
      op = activePuts.remove(message.getUUID());
    } else if (activeRetrievals.containsKey(message.getUUID())) {
      op = activeRetrievals.remove(message.getUUID());
    }
    if (op != null) {
      try {
        connection.sendAsynchronous(message,
            SystemTimeUtil.skSystemTimeSource.absTimeMillis() + message.getDeadlineRelativeMillis());
      } catch (IOException ioe) {
        log.warn("Error whiles sending message",ioe);
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
        log.warn("getReplicaListPair: {} ", key);
      }
      // FUTURE - think about improvements
      replicaListPair = ringMaster.getReplicaListPair(key, ownerQueryOpType);
      if (debug) {
        log.debug("{} \t {} :", key, replicaListPair);
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
        log.warn("getReplicas {} \t {}", key, oqm);
      }
      // FUTURE - think about improvements
      replicaList = ringMaster.getReplicaList(key, oqm, ownerQueryOpType);
      if (debug) {
        log.warn("{} \t {}", key, CollectionUtil.toString(replicaList, ':'));
      }
      return replicaList;
    }
  }

  @Override
  public IPAndPort[] getReplicas(DHTKey key, OwnerQueryMode oqm, RingOwnerQueryOpType ownerQueryOpType) {
    IPAndPort[] replicas;

    if (debug) {
      log.warn("getPrimaryReplicas {}", key);
    }
    // FUTURE - think about improvements
    replicas = ringMaster.getReplicas(key, oqm, ownerQueryOpType);
    if (debug) {
      log.warn("{} \t {}", key, IPAndPort.arrayToString(replicas));
    }
    return replicas;
  }

  public IPAndPort localIPAndPort() {
    return myIPAndPort;
  }

  @Override
  public boolean isLocal(IPAndPort replica) {
    if (debug) {
      log.warn("#### {} {}\t{}", replica, myIPAndPort, replica.equals(myIPAndPort));
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

    if (log.isDebugEnabled()) {
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

    if (log.isDebugEnabled()) {
      message.displayForDebug();
    }
    log.warn("handleSyncRequest");
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
    log.warn("requestChecksumTree");
    throw new RuntimeException("deprecated testing"); // FUTURE remove after double checking
  }

  private void handleChecksumTreeRequest(MessageGroup message, MessageGroupConnection connection) {
    ConvergencePoint targetCP;
    ConvergencePoint sourceCP;
    RingRegion region;
    boolean localFlag;

    if (log.isDebugEnabled()) {
      log.warn("handleChecksumTreeRequest");
      message.displayForDebug();
    }
    targetCP = ProtoChecksumTreeRequestMessageGroup.getTargetConvergencePoint(message);
    sourceCP = ProtoChecksumTreeRequestMessageGroup.getSourceConvergencePoint(message);
    region = ProtoChecksumTreeRequestMessageGroup.getRegion(message);
    localFlag = ProtoChecksumTreeRequestMessageGroup.getLocalFlag(message);
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
    if (log.isDebugEnabled()) {
      log.warn("handleIncomingChecksumTree");
      message.displayForDebug();
    }
    storage.asyncInvocationNonBlocking("incomingChecksumTree", message, connection);
  }

  ////////////////////////////

  private void handleNamespaceRequest(MessageGroup message, MessageGroupConnection connection) {
    storage.handleNamespaceRequest(message, connection);
  }

  private void handleNamespaceResponse(MessageGroup message, MessageGroupConnection connection) {
    storage.asyncInvocationNonBlocking("handleNamespaceResponse", message, connection);
  }

  private void handleSetConvergenceState(MessageGroup message, MessageGroupConnection connectionForRemote) {
    storage.handleSetConvergenceState(message, connectionForRemote);
  }

  private void handleReap(MessageGroup message, MessageGroupConnection connectionForRemote) {
    storage.handleReap(message, connectionForRemote);
  }

  ////////////////////////////
    
  private void handleNop(MessageGroup message, MessageGroupConnectionProxy connection) {
    log.debug("{} {}", message.getMessageType(), connection.getConnectionID());
  }

  private void handlePing(MessageGroup message, MessageGroupConnectionProxy connection) {
    if (log.isDebugEnabled()) {
      log.debug("{} {} {} {}", message.getMessageType(), connection.getConnectionID(), message.getUUID(),
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
          log.info("Requesting ping of excluded daemon: {}", source);
          pingerThread.requestPing(source);
        } else {
          log.info("No pinger thread available, not requesting ping of excluded daemon: {}", source);
        }
      }
      // Special case for ping messages - we send ip and port as originator
      ack = new ProtoPingAckMessageGroup(myIPAndPortAsOriginator, message.getUUID());
      c = (MessageGroupConnectionProxyRemote) connection;
      try {
        c.sendAsynchronous(ack.toMessageGroup(), Long.MAX_VALUE);
      } catch (IOException ioe) {
        log.warn("Error while sending message",ioe);
      }
    }
  }

  private void handlePingAck(MessageGroup message, MessageGroupConnectionProxy connection) {
    if (log.isDebugEnabled()) {
      log.debug("{} {} {} {}", message.getMessageType(), connection.getConnectionID(), message.getUUID(),
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
      log.warn("Priming: {}", replica);
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

  public ExclusionChangeListener getExclusionChangeListener() {
    return exclusionChangeListener;
  }
  
  private class ExclusionChangeListener
      implements Listener<Triple<Set<IPAndPort>, Set<IPAndPort>, Set<IPAndPort>>>, Comparable<ExclusionChangeListener> {
    @Override
    public void notification(Broadcaster<Triple<Set<IPAndPort>, Set<IPAndPort>, Set<IPAndPort>>> broadcaster,
        Triple<Set<IPAndPort>, Set<IPAndPort>, Set<IPAndPort>> message) {
      notifyOfReplicaChange(message.getV2(), message.getV3());
    }

    @Override
    public int compareTo(ExclusionChangeListener o) {
      if (o == this) {
        return 0;
      } else {
        throw new RuntimeException("Only one MessageModule expected presently");
      }
    }
  }
  
  private void notifyOfReplicaChange(Set<IPAndPort> newlyExcludedReplicas, Set<IPAndPort> newlyIncludedReplicas) {
    long    absTimeMillis;

    absTimeMillis = absMillisTimeSource.absTimeMillis(); 
    // Note: put notification for the purpose of retrying puts would require holding on to the value in the proxy 
    // which has been shown to cause oom-induced crashes.
    // puts could be notified for the purpose of determining that a put has succeeded; we presently handle this
    // case in the client
    notifyOfReplicaChange(activeRetrievals, absTimeMillis, newlyExcludedReplicas, newlyIncludedReplicas);
  }
  
  private void notifyOfReplicaChange(ConcurrentMap<UUIDBase, ? extends ActiveProxyOperation<?, ?>> map,
      long absTimeMillis, Set<IPAndPort> newlyExcludedReplicas, Set<IPAndPort> newlyIncludedReplicas) {
    log.warn("MessageModule.notifyOfReplicaChange ex {} in {}", CollectionUtil.toString(newlyExcludedReplicas),
        CollectionUtil.toString(newlyIncludedReplicas));
    if (newlyExcludedReplicas.isEmpty() && newlyIncludedReplicas.isEmpty()) {
      return;
    } else {
      for (Map.Entry<UUIDBase, ? extends ActiveProxyOperation<?, ?>> entry : map.entrySet()) {
        if (entry.getValue().hasTimedOut(
            absTimeMillis) || entry.getValue().getOpResult().isComplete()) { // FIXME - think about failures
          if (debugCleanup) {
            System.out.printf("Removing %s\n", entry.getKey());
          }
          map.remove(entry.getKey());
        } else {
          entry.getValue().exclusionsChanged(newlyExcludedReplicas, newlyIncludedReplicas);
        }
      }
    }
  }

  /////////////////////////////////

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
        log.info("Cleaning MessageModule maps");
      }
      absTimeMillis = absMillisTimeSource.absTimeMillis();
      cleanupMap(activePuts, absTimeMillis);
      cleanupMap(activeRetrievals, absTimeMillis);
      if (debugCleanup) {
        log.info("Done cleaning MessageModule maps");
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
      log.warn("Pinger running");
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
      Set<IPAndPort> _pingRequests;
      Set<IPAndPort> pingTargets;

      log.debug("Pinging replicas");
      _pingRequests = ImmutableSet.copyOf(pingRequests);
      pingRequests.removeAll(_pingRequests);
      pingTargets = new HashSet<>(ringMaster.getAllCurrentAndTargetNonExcludedNonLocalReplicaServers());
      pingTargets.addAll(_pingRequests);
      for (IPAndPort replica : pingTargets) {
        log.debug("Pinging {}", replica);
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
