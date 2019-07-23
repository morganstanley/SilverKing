package com.ms.silverking.cloud.dht.client.impl;

import java.io.IOException;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.cloud.dht.NamespaceCreationOptions;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespacePerspectiveOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.WaitOptions;
import com.ms.silverking.cloud.dht.client.AsyncRetrieval;
import com.ms.silverking.cloud.dht.client.AsyncSingleValueRetrieval;
import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.NamespaceCreationException;
import com.ms.silverking.cloud.dht.client.NamespaceDeletionException;
import com.ms.silverking.cloud.dht.client.NamespaceRecoverException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.SessionEstablishmentTimeoutController;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.serialization.SerializationRegistry;
import com.ms.silverking.cloud.dht.common.Context;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.DHTUtil;
import com.ms.silverking.cloud.dht.common.NamespaceOptionsClient;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.common.NamespaceUtil;
import com.ms.silverking.cloud.dht.common.TimeoutException;
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceNotCreatedException;
import com.ms.silverking.cloud.dht.meta.MetaClient;
import com.ms.silverking.cloud.dht.meta.NamespaceLinksZK;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.net.MessageGroupConnection;
import com.ms.silverking.cloud.dht.net.MessageGroupReceiver;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.net.async.QueueingConnectionLimitListener;
import com.ms.silverking.thread.lwt.BaseWorker;
import com.ms.silverking.time.AbsMillisTimeSource;

/**
 * Concrete implementation of DHTSession. 
 */
public class DHTSessionImpl implements DHTSession, MessageGroupReceiver, QueueingConnectionLimitListener {
    private final MessageGroupBase    mgBase;
    private final ClientDHTConfiguration    dhtConfig;
    //private final ServerPool        serverPool;
    private final ConcurrentMap<Long,ClientNamespace>  clientNamespaces;
    private final List<ClientNamespace> clientNamespaceList;
    private final byte[]            myIPAndPort;
    private final AbsMillisTimeSource   absMillisTimeSource;
    private final SerializationRegistry serializationRegistry;
    private final Worker            worker;
    private final NamespaceCreator  namespaceCreator;
    private final NamespaceOptionsClient    nsOptionsClient;
    private NamespaceLinkMeta nsLinkMeta;
    
    private AsynchronousNamespacePerspective<String,String>    systemNSP;
    private ExclusionSet    exclusionSet;
    
    /*
     * FUTURE - This class can be improved significantly. It contains remnants of the ActiveOperation* implementation.
     * Also to be decided is whether or not the server will share common functionality with the client
     * side. It currently does not, but it did when the ActiveOperation* implementation was in place.
     */
    
    
    // FUTURE - server selection currently pinned to preferredServer only; allow for others
    private AddrAndPort  server;
    
    //private final Map<OperationUUID,ActiveOperation>    activeOps;
    
    // new implementation
    //private final ActiveOperationTable   activeOpTable;
    // FUTURE - THINK IF WE WANT OPERATION TABLE ANY MORE
    
    // retrieve
    // map of retrievals ns, key to retrieval list
        // this list then maps back to active operation
        // retrieval list maps version to active operation
    
    // put
    // map of puts ns, key to put list
    
    
    // end new implementation
    
    private static final int    timeoutCheckIntervalMillis = 4 * 1000;
    private static final int    serverCheckIntervalMillis = 2 * 60 * 1000;
    private static final int    serverOrderIntervalMillis = 5 * 60 * 1000;
    private static final int    timeoutExclusionSetRetrievalMillis = 1000;
    
    private static final int   connectionQueueLimit = 0;
    
    private static final int   numSelectorControllers = 1;
    private static final String selectorControllerClass = "DHTSessionImpl";
    
    public DHTSessionImpl(ClientDHTConfiguration dhtConfig, 
                          AddrAndPort preferredServer,
                          AbsMillisTimeSource absMillisTimeSource, 
                          SerializationRegistry serializationRegistry, 
                          SessionEstablishmentTimeoutController timeoutController) throws IOException {
        mgBase = new MessageGroupBase(0, this, absMillisTimeSource, new NewConnectionTimeoutControllerWrapper(timeoutController), 
                                      this, connectionQueueLimit, numSelectorControllers, selectorControllerClass);
        mgBase.enable();
        server = preferredServer;
        // Eagerly create the connection so that failures occur here, rather than after the session object is returned
        if (!DHTConstants.isDaemon) {
            mgBase.ensureConnected(preferredServer);
        }
        this.dhtConfig = dhtConfig;
        this.absMillisTimeSource = absMillisTimeSource;
        this.serializationRegistry = serializationRegistry;
        myIPAndPort = IPAddrUtil.createIPAndPort(IPAddrUtil.localIP(), mgBase.getPort());
        Log.info("Session IP:Port ", IPAddrUtil.addrAndPortToString(myIPAndPort));
        
        clientNamespaces = new ConcurrentHashMap<>();  
        clientNamespaceList = new CopyOnWriteArrayList<>();
        
        worker = new Worker();
        DHTUtil.timer().scheduleAtFixedRate(new TimeoutCheckTask(), 
                                              timeoutCheckIntervalMillis, 
                                              timeoutCheckIntervalMillis);
        namespaceCreator = new SimpleNamespaceCreator();
        nsOptionsClient = new NamespaceOptionsClient(this, dhtConfig, timeoutController);
    }
    
    MessageGroupBase getMessageGroupBase() {
        return mgBase;
    }
    
    @Override
    public NamespaceCreationOptions getNamespaceCreationOptions() {
        return nsOptionsClient.getNamespaceCreationOptions();
    }
    
    @Override
    public NamespaceOptions getDefaultNamespaceOptions() {
        return getNamespaceCreationOptions().getDefaultNamespaceOptions();
    }
    
    @Override
    public PutOptions getDefaultPutOptions() {
        return getDefaultNamespaceOptions().getDefaultPutOptions();
    }

    @Override
    public GetOptions getDefaultGetOptions() {
        return getDefaultNamespaceOptions().getDefaultGetOptions();
    }

    @Override
    public WaitOptions getDefaultWaitOptions() {
        return getDefaultNamespaceOptions().getDefaultWaitOptions();
    }
    
    private NamespaceLinkMeta getNSLinkMeta() {
        synchronized (this) {
            if (nsLinkMeta == null) {
                try {
                    MetaClient      mc;
    
                    mc = new MetaClient(dhtConfig.getName(), dhtConfig.getZKConfig());
                    nsLinkMeta = new NamespaceLinkMeta(new NamespaceLinksZK(mc));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return nsLinkMeta;
        }
    }

    @Override
    public void queueAboveLimit() {
        for (ClientNamespace clientNamespace : clientNamespaceList) {
            clientNamespace.queueAboveLimit();
        }
    }

    @Override
    public void queueBelowLimit() {
        for (ClientNamespace clientNamespace : clientNamespaceList) {
            clientNamespace.queueBelowLimit();
        }
    }
    
    private NamespaceProperties getNamespaceProperties(String namespace) {
        if (namespace.equals(NamespaceUtil.metaInfoNamespaceName)) {
            return NamespaceUtil.metaInfoNamespaceProperties;
        } else {
            try {
                return nsOptionsClient.getNamespaceProperties(namespace);
            } catch (TimeoutException te) {
                throw new RuntimeException("Timeout retrieving namespace meta information "+ 
                        Long.toHexString(NamespaceUtil.nameToLong(namespace)) +" "+ namespace, te);
            } catch (RetrievalException re) {
                SynchronousNamespacePerspective<Long,String>  syncNSP;
                String  locations;
                long    ns;
                
                ns = NamespaceUtil.nameToLong(namespace);
                syncNSP = getNamespace(Namespace.replicasName).openSyncPerspective(Long.class, String.class);
                try {
                    locations = syncNSP.get(ns);
                    Log.warning("Failed to retrieve namespace "+ String.format("%x", ns));
                    Log.warning(locations);
                } catch (RetrievalException re2) {
                    Log.warning(re2.getDetailedFailureMessage());
                    Log.warning("Unexpected failure attempting to find key locations "
                               +"during failed ns retrieval processing");
                }
                Log.warning(re.getDetailedFailureMessage());
                throw new RuntimeException("Unable to retrieve namespace meta information "+ Long.toHexString(ns), re);
            }
        }
    }
    
    private ClientNamespace getClientNamespace(String namespace) {
        ClientNamespace clientNamespace;
        Context         context;
        
        context = namespaceCreator.createNamespace(namespace);
        clientNamespace = clientNamespaces.get(context.contextAsLong());
        if (clientNamespace == null) {
            ClientNamespace previous;
            NamespaceProperties nsProperties;
            NamespaceOptions    nsOptions;
            ClientNamespace     parent;
            NamespaceLinkMeta   nsLinkMeta;
            
            nsProperties = getNamespaceProperties(namespace);
            if (nsProperties == null) {
                throw new NamespaceNotCreatedException(namespace);
            }
            nsOptions = nsProperties.getOptions();
            if (nsProperties.getParent() != null) {
                parent = getClientNamespace(nsProperties.getParent());
            } else {
                parent = null;
            }
            if (nsOptions.getAllowLinks() && nsOptions.getVersionMode() == NamespaceVersionMode.SINGLE_VERSION) {
                nsLinkMeta = getNSLinkMeta();
            } else {
                nsLinkMeta = null;
            }
            clientNamespace = new ClientNamespace(this, namespace, nsOptions, 
                                        serializationRegistry, absMillisTimeSource, server, parent, 
                                        nsLinkMeta);
            previous = clientNamespaces.putIfAbsent(context.contextAsLong(), clientNamespace);
            if (previous != null) {
                clientNamespace = previous;
            } else {
                if (Log.levelMet(Level.INFO)) {
                    Log.info("Created client namespace: "+ namespace +" "+ context);
                }
                clientNamespaceList.add(clientNamespace);
            }
        }
        return clientNamespace;
    }
    
    @Override
    public Namespace createNamespace(String namespace) throws NamespaceCreationException {
        return createNamespace(namespace, getNamespaceCreationOptions().getDefaultNamespaceOptions());
    }
    
    @Override
    public Namespace createNamespace(String namespace, NamespaceOptions nsOptions) throws NamespaceCreationException {
        if (nsOptions == null) {
            nsOptions = getNamespaceCreationOptions().getDefaultNamespaceOptions();
        }
        return createNamespace(namespace, new NamespaceProperties(nsOptions));
    }
    
    Namespace createNamespace(String namespace, NamespaceProperties nsProperties) throws NamespaceCreationException {
        nsOptionsClient.createNamespace(namespace, nsProperties);
        return getClientNamespace(namespace);
    }
    
    @Override 
    public Namespace getNamespace(String namespace) {
        return getClientNamespace(namespace);
    }
    
    @Override
    public void deleteNamespace(String namespace) throws NamespaceDeletionException {
        // Placeholder for future implementation
        /*
        try {
            //GlobalCommandZK zk;
            //MetaClient      mc;
            
            //mc = new MetaClient(dhtConfig.getName(), new ZooKeeperConfig(dhtConfig.getZkLocs()));        
            //zk = new GlobalCommandZK(mc);
            // FUTURE - we don't want this here, we need another class to execute the command
            // and track its completion
        } catch (IOException | KeeperException e) {
            throw new NamespaceDeletionException(e);
        }
        */
    }

    @Override
    public void recoverNamespace(String namespace) throws NamespaceRecoverException {
    }
    
    @Override
    public <K, V> AsynchronousNamespacePerspective<K, V> openAsyncNamespacePerspective(String namespace, 
                                                                    NamespacePerspectiveOptions<K,V> nspOptions) {
        return new AsynchronousNamespacePerspectiveImpl<K,V>(getClientNamespace(namespace), namespace,
                                                               new NamespacePerspectiveOptionsImpl<>(nspOptions, serializationRegistry));
    }
    
    @Override
    public <K, V> AsynchronousNamespacePerspective<K, V> openAsyncNamespacePerspective(
                                                            String namespace, Class<K> keyClass, Class<V> valueClass) {
        ClientNamespace    ns;
        
        ns = getClientNamespace(namespace);
        return new AsynchronousNamespacePerspectiveImpl<K,V>(ns, namespace,
                new NamespacePerspectiveOptionsImpl<>(ns.getDefaultNSPOptions(keyClass, valueClass), serializationRegistry));
    }

    @Override
    public AsynchronousNamespacePerspective<String,byte[]> openAsyncNamespacePerspective(String namespace) {
        return openAsyncNamespacePerspective(namespace, DHTConstants.defaultKeyClass, DHTConstants.defaultValueClass);
    }
    
    @Override
    public <K, V> SynchronousNamespacePerspective<K, V> openSyncNamespacePerspective(String namespace, 
                                                                    NamespacePerspectiveOptions<K,V> nspOptions) {
        return new SynchronousNamespacePerspectiveImpl<K, V>(getClientNamespace(namespace), namespace, 
                new NamespacePerspectiveOptionsImpl<>(nspOptions, serializationRegistry));
    }

    @Override
    public <K, V> SynchronousNamespacePerspective<K, V> openSyncNamespacePerspective(
                                                            String namespace, Class<K> keyClass, Class<V> valueClass) {
        ClientNamespace    ns;
        
        ns = getClientNamespace(namespace);
        return new SynchronousNamespacePerspectiveImpl<K, V>(ns, namespace, 
                new NamespacePerspectiveOptionsImpl<>(ns.getDefaultNSPOptions(keyClass, valueClass), serializationRegistry));
    }
    
    @Override
    public SynchronousNamespacePerspective<String,byte[]> openSyncNamespacePerspective(String namespace) {
        return openSyncNamespacePerspective(namespace, DHTConstants.defaultKeyClass, DHTConstants.defaultValueClass);
    }

    @Override
    public void close() {
        mgBase.shutdown();
        // FUTURE - consider additional actions
    }
    
    static class MessageAndConnection {
        final MessageGroup             message;
        final MessageGroupConnection   connection;
        
        MessageAndConnection(MessageGroup message, MessageGroupConnection connection) {
            this.message = message;
            this.connection = connection;
        }
    }
    
    class Worker extends BaseWorker<MessageAndConnection> {
        Worker() {
        }
        
        @Override
        public void doWork(MessageAndConnection mac) {
            _receive(mac.message, mac.connection);
        }   
    }
    
    @Override
    public void receive(MessageGroup message, MessageGroupConnection connection) {
        _receive(message, connection);
        //worker.addWork(new MessageAndConnection(message, connection));
        // FUTURE - add intelligence about when to do work directly and when to use a worker
    }
    
    private void _receive(MessageGroup message, MessageGroupConnection connection) {
        ClientNamespace    clientNamespace;
        
        Log.fine("received from ", connection);
        clientNamespace = clientNamespaces.get(message.getContext());
        if (clientNamespace != null) {
            clientNamespace.receive(message, connection);
        } else {
            Log.warning("No context found for: ", message);
        }
    }
    
    /////////////////////
    
    void initializeExclusionSet() {
        try {
            if (systemNSP == null) {
                systemNSP = getClientNamespace(Namespace.systemName).openAsyncPerspective(String.class, String.class);
            }
            exclusionSet = getCurrentExclusionSet();
        } catch (Exception e) {
            Log.logErrorWarning(e, "initializeExclusionSet() failed");
        }
    }
    
    ExclusionSet getCurrentExclusionSet() {
        try {
            String    exclusionSetDef;
            AsyncSingleValueRetrieval<String,String> asyncRetrieval;
            boolean       complete;

            asyncRetrieval = systemNSP.get("exclusionSet");
            complete = asyncRetrieval.waitForCompletion(timeoutExclusionSetRetrievalMillis, TimeUnit.MILLISECONDS);
            if (complete) {
              exclusionSetDef = String.valueOf(asyncRetrieval.getStoredValue());
            } else {
              exclusionSetDef = null;
            }
            if (exclusionSetDef != null) {
                  return ExclusionSet.parse(exclusionSetDef);
              } else {
                  return null;
              }

        } catch (Exception e) {
            Log.logErrorWarning(e, "getCurrentExclusionSet() failed");
            return null;
        }
    }
    
    boolean exclusionSetHasChanged() {
        if (exclusionSet == null) {
            initializeExclusionSet();
            return false;
        } else {
            ExclusionSet    newExclusionSet;
            boolean            exclusionSetHasChanged;
            
            newExclusionSet = getCurrentExclusionSet();
            exclusionSetHasChanged = newExclusionSet == null || !exclusionSet.equals(newExclusionSet);
            exclusionSet = newExclusionSet;
            return exclusionSetHasChanged;
        }
    }
            
    void checkForTimeouts() {
        long    curTimeMillis;
        boolean    exclusionSetHasChanged;
        
        curTimeMillis = absMillisTimeSource.absTimeMillis();
        exclusionSetHasChanged = exclusionSetHasChanged();
        for (ClientNamespace clientNamespace : clientNamespaceList) {
            clientNamespace.checkForTimeouts(curTimeMillis, exclusionSetHasChanged);
        }
    }
        
    public class TimeoutCheckTask extends TimerTask {
        public void run() {
            try {
                checkForTimeouts();
            } catch (Exception e) {
                Log.logErrorWarning(e);
            }
        }
    }
}
