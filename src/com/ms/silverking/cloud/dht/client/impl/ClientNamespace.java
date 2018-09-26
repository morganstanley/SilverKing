package com.ms.silverking.cloud.dht.client.impl;

import java.util.List;
import java.util.logging.Level;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespacePerspectiveOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.client.AbsMillisVersionProvider;
import com.ms.silverking.cloud.dht.client.AbsNanosVersionProvider;
import com.ms.silverking.cloud.dht.client.AsynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.ConstantVersionProvider;
import com.ms.silverking.cloud.dht.client.KeyDigestType;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.NamespaceCreationException;
import com.ms.silverking.cloud.dht.client.NamespaceLinkException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.VersionProvider;
import com.ms.silverking.cloud.dht.client.serialization.SerializationRegistry;
import com.ms.silverking.cloud.dht.common.Context;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceNotCreatedException;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.net.MessageGroupConnection;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.net.async.QueueingConnectionLimitListener;
import com.ms.silverking.time.AbsMillisTimeSource;

public class ClientNamespace implements QueueingConnectionLimitListener, Namespace {
    private final DHTSessionImpl    session;
    private final String            name;
    private final NamespaceOptions  nsOptions;
    private final Context           context;
    private final ActiveClientOperationTable  activeOpTable;
    private final AbsMillisTimeSource   absMillisTimeSource;
    private final OpSender          opSender;
    private final OpSender          putSender;
    private final OpSender          retrievalSender;
    private final byte[]            originator;
    private final SerializationRegistry serializationRegistry;
    private final Namespace         parent;
    private final NamespaceLinkMeta nsLinkMeta;
    
    protected enum OpLWTMode {AllowUserThreadUsage, DisallowUserThreadUsage;

    public int getDirectCallDepth() {
        switch (this) {
        case AllowUserThreadUsage: return Integer.MAX_VALUE;
        case DisallowUserThreadUsage: return 0;
        default: throw new RuntimeException("panic");
        }
    }};

    /*
     * The current implementation allows puts which are doomed to fail to continue.
     * They will eventually be rejected by the storage node. We don't
     * avoid this currently since this is expected to be a rare case.
     * 
     * For receives, however, we track versions more carefully in order to
     * group concurrent receives together. ActiveOperationTable
     * takes care of this.
     * 
     * FUTURE - think about whether we want to keep ActiveOperationTable.
     * Seems like put/retrieve should use similar approach more.
     */
    
    ClientNamespace(DHTSessionImpl session, String name, NamespaceOptions nsOptions,
                SerializationRegistry serializationRegistry,
                AbsMillisTimeSource absMillisTimeSource, AddrAndPort server, 
                Namespace parent, NamespaceLinkMeta nsLinkMeta) {
        MessageGroupBase  mgBase;
        
        mgBase = session.getMessageGroupBase();
        this.session = session;
        this.name = name;
        this.nsOptions = nsOptions;
        this.serializationRegistry = serializationRegistry;
        this.absMillisTimeSource = absMillisTimeSource;
        context = new SimpleNamespaceCreator().createNamespace(name);
        activeOpTable = new ActiveClientOperationTable();
        opSender = new OpSender(server, mgBase);
        putSender = new OpSender(server, mgBase);
        retrievalSender = new OpSender(server, mgBase);
        originator = mgBase.getMyID();
        this.parent = parent;
        this.nsLinkMeta = nsLinkMeta;
        if (nsOptions.getVersionMode() != NamespaceVersionMode.SINGLE_VERSION || !nsOptions.getAllowLinks()) {
            assert nsLinkMeta == null;
        }
    }

    @Override
    public String getName() {
        return name;
    }
    
    Context getContext() {
        return context;
    }
    
    @Override
    public NamespaceOptions getOptions() {
        return nsOptions;
    }
    
	@Override
	public <K, V> NamespacePerspectiveOptions<K, V> getDefaultNSPOptions(Class<K> keyClass, Class<V> valueClass) {
		VersionProvider	versionProvider;
		
		// FUTURE - Currently we intercept request for NS-supplied versions and provide them here.
		// We may want to provide them on the server. Code for this is already in ActiveProxyPut.
		switch (nsOptions.getVersionMode()) {
		case SINGLE_VERSION: 
			// SINGLE_VERSION internally expects the version number to be abs millis
			versionProvider = new AbsMillisVersionProvider(SystemTimeUtil.systemTimeSource);
			break;
		case CLIENT_SPECIFIED:
			versionProvider = new ConstantVersionProvider(SystemTimeUtil.systemTimeSource.absTimeMillis());
			break;
		case SYSTEM_TIME_MILLIS:
			versionProvider = new AbsMillisVersionProvider(SystemTimeUtil.systemTimeSource);
			break;
		case SEQUENTIAL: // FIXME - for now treat sequential as nanos
		case SYSTEM_TIME_NANOS:
			versionProvider = new AbsNanosVersionProvider(SystemTimeUtil.systemTimeSource);
			break;
	    default: throw new RuntimeException("panic");
		}
		
		return new NamespacePerspectiveOptions<K,V>(keyClass, valueClass, 
												KeyDigestType.MD5, // FUTURE - centralize 
												nsOptions.getDefaultPutOptions(),
												nsOptions.getDefaultInvalidationOptions(),
												nsOptions.getDefaultGetOptions(), 
												nsOptions.getDefaultWaitOptions(), 
												versionProvider, null);
	}

    DHTSessionImpl getSession() {
        return session;
    }
    
    public AbsMillisTimeSource getAbsMillisTimeSource() {
        return absMillisTimeSource;
    }
    
    public byte[] getOriginator() {
        return originator;
    }
    
    public ActivePutListeners getActivePutListeners() {
        return activeOpTable.getActivePutListeners();
    }
    
    public ActiveRetrievalListeners getActiveRetrievalListeners() {
        return activeOpTable.getActiveRetrievalListeners();
    }
    
    public ActiveVersionedBasicOperations getActiveVersionedBasicOperations() {
        return activeOpTable.getActiveVersionedBasicOperations();
    }
    
    OpSender getRetrievalSender() {
        return retrievalSender;
    }
    
    @Override
    public void queueAboveLimit() {
        putSender.pause();
        retrievalSender.pause();
    }

    @Override
    public void queueBelowLimit() {
        putSender.unpause();
        retrievalSender.unpause();
    }
    
    // operation
    
    public <K, V> void startOperation(AsyncOperationImpl opImpl, OpLWTMode opLWTMode) {
        switch (opImpl.getType()) {
        case RETRIEVE: retrievalSender.addWorkForGrouping(opImpl, opLWTMode.getDirectCallDepth()); break;
        case PUT: putSender.addWorkForGrouping(opImpl, opLWTMode.getDirectCallDepth()); break;
        default: opSender.addWorkForGrouping(opImpl, 0); break; // FIXME - don't group
        }
    }

    // receive
    
    public void receive(MessageGroup message, MessageGroupConnection connection) {
        if (Log.levelMet(Level.FINE)) {
            Log.warning("\t*** Received: ", message);
            message.displayForDebug(false);
            //message.displayForDebug(true);
        }
        switch (message.getMessageType()) {
        case PUT_RESPONSE:
            activeOpTable.getActivePutListeners().receivedPutResponse(message);
            break;
        case RETRIEVE_RESPONSE:
            //activeOpTable.receivedRetrievalResponse(message);
            activeOpTable.getActiveRetrievalListeners().receivedRetrievalResponse(message);
            break;
        case OP_RESPONSE:
            activeOpTable.getActiveVersionedBasicOperations().receivedOpResponse(message);
            break;
        case CHECKSUM_TREE: // FUTURE - for testing, consider removing
            activeOpTable.receivedChecksumTree(message); // FUTURE - for testing, consider removing
            break;
        default: throw new RuntimeException("Unexpected message type: "+ message.getMessageType());
        }
    }
    
    void checkForTimeouts(long curTimeMillis, boolean exclusionSetHasChanged) {
        Log.info("checkForTimeouts: ", name);
        activeOpTable.checkForTimeouts(curTimeMillis, opSender, putSender, retrievalSender, exclusionSetHasChanged);
    }
    
    List<AsyncOperationImpl> getActiveAsyncOperations() {
        return activeOpTable.getActiveAsyncOperations();
    }
    
    @Override
    public <K, V> AsynchronousNamespacePerspective<K, V> openAsyncPerspective(NamespacePerspectiveOptions<K,V> nspOptions) {
        return new AsynchronousNamespacePerspectiveImpl<K,V>(this, name,
                                         new NamespacePerspectiveOptionsImpl<>(nspOptions, serializationRegistry));
    }

	@Override
	public <K, V> AsynchronousNamespacePerspective<K, V> openAsyncPerspective(
			Class<K> keyClass, Class<V> valueClass) {
        return new AsynchronousNamespacePerspectiveImpl<K,V>(this, name,
                new NamespacePerspectiveOptionsImpl<>(getDefaultNSPOptions(keyClass, valueClass), serializationRegistry));
	}
	
	@Override
	public AsynchronousNamespacePerspective<String,byte[]> openAsyncPerspective() {
		return openAsyncPerspective(DHTConstants.defaultKeyClass, DHTConstants.defaultValueClass);
	}
	
	
    @Override
    public <K, V> SynchronousNamespacePerspective<K, V> openSyncPerspective(
                                                                    NamespacePerspectiveOptions<K,V> nspOptions) {
        return new SynchronousNamespacePerspectiveImpl<K, V>(this, name, 
                new NamespacePerspectiveOptionsImpl<>(nspOptions, serializationRegistry));
    }

	@Override
	public <K, V> SynchronousNamespacePerspective<K, V> openSyncPerspective(
			Class<K> keyClass, Class<V> valueClass) {
        return new SynchronousNamespacePerspectiveImpl<K, V>(this, name, 
                new NamespacePerspectiveOptionsImpl<>(getDefaultNSPOptions(keyClass, valueClass), serializationRegistry));
	}
    
	@Override
	public SynchronousNamespacePerspective<String,byte[]> openSyncPerspective() {
		return openSyncPerspective(DHTConstants.defaultKeyClass, DHTConstants.defaultValueClass);
	}
    
    // misc.

    @Override
    public String toString() {
        return name;
    }

    @Override
    public Namespace clone(String childName) throws NamespaceCreationException {
        long    creationTime;
        long    minVersion;
        
        creationTime = SystemTimeUtil.systemTimeSource.absTimeNanos();
        switch (nsOptions.getVersionMode()) {
        case SINGLE_VERSION:
            minVersion = 0;
            break;
        case CLIENT_SPECIFIED:
            throw new RuntimeException("Namespace.clone() must be provided a version "
                                       +"for version mode of CLIENT_SPECIFIED");
        case SEQUENTIAL:
            throw new RuntimeException("Namespace.clone() not supported for version mode of SEQUENTIAL");
        case SYSTEM_TIME_MILLIS:
            minVersion = SystemTimeUtil.systemTimeSource.absTimeMillis();
            break;
        case SYSTEM_TIME_NANOS:
            minVersion = SystemTimeUtil.systemTimeSource.absTimeNanos();
            break;
        default: throw new RuntimeException("Panic");
        }
        return session.createNamespace(childName, new NamespaceProperties(nsOptions, this.name, minVersion, creationTime));
    }

    @Override
    public Namespace clone(String childName, long minVersion) throws NamespaceCreationException {
        long    creationTime;
        
        creationTime = SystemTimeUtil.systemTimeSource.absTimeNanos();
        switch (nsOptions.getVersionMode()) {
        case CLIENT_SPECIFIED:
            break;
        case SEQUENTIAL:
            throw new RuntimeException("Namespace.clone() not supported for version mode of SEQUENTIAL");
        case SINGLE_VERSION:
        case SYSTEM_TIME_MILLIS:
        case SYSTEM_TIME_NANOS:
            throw new RuntimeException("Namespace.clone() can only be supplied a version "
                                       +"for a version mode of CLIENT_SPECIFIED");
        default: throw new RuntimeException("Panic");
        }
        return session.createNamespace(childName, new NamespaceProperties(nsOptions, this.name, minVersion, creationTime));
    }
    
    public void linkTo(String parentName) throws NamespaceLinkException {
        Namespace   parent;

        if (!nsOptions.getAllowLinks()) {
            throw new NamespaceLinkException("Links not allowed for: "+ name);
        }
        
        try {
            parent = session.getNamespace(parentName);
        } catch (NamespaceNotCreatedException nnce) {
            throw new NamespaceLinkException("Parent not created: "+ parentName);
        }
        
        if (!parent.getOptions().equals(nsOptions)) {
            throw new NamespaceLinkException("Parent and child options differ:\n"+ 
                                             parent.getOptions() +"\n"+ nsOptions);
        }
        
        if (nsOptions.getVersionMode() != NamespaceVersionMode.SINGLE_VERSION) {
            throw new NamespaceLinkException("Namespace.linkTo() only supported for write-once namespaces");
        } else {
            nsLinkMeta.createLink(name, parentName);
        }
    }
}
