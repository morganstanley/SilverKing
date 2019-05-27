package com.ms.silverking.cloud.dht.common;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ms.silverking.cloud.dht.SessionOptions;
import com.ms.silverking.cloud.dht.client.ClientDHTConfigurationProvider;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.impl.ActiveClientOperationTable;
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceNotCreatedException;
import com.ms.silverking.log.Log;

/**
 * Provides NamespaceOptions for the StorageModule. Internally uses NamespaceOptionsClient to retrieve options.
 */
public class NamespaceMetaStore {
    private final DHTSession                session;
    private final NamespaceOptionsClient    nsOptionsClient;
    private final ConcurrentMap<Long, NamespaceProperties>  nsPropertiesMap;
    
    private static final long	nsOptionsFetchTimeoutMillis = SessionOptions.getDefaultTimeoutController().getMaxRelativeTimeoutMillis(null);	
    
    public enum NamespaceOptionsRetrievalMode {FetchRemotely, LocalCheckOnly};
    
    private static final boolean   debug = true;
    
    public NamespaceMetaStore(DHTSession session) {
        this.session = session;
        try {
            ActiveClientOperationTable.disableFinalization();
            nsOptionsClient = new NamespaceOptionsClient(session);
            nsPropertiesMap = new ConcurrentHashMap<>();
        } catch (Exception e) {
            throw new RuntimeException("Unexpected exception", e);
        }
    }
    
    public static NamespaceMetaStore create(ClientDHTConfigurationProvider dhtConfigProvider) {
        try {
            return new NamespaceMetaStore(new DHTClient().openSession(dhtConfigProvider));
        } catch (Exception e) {
            throw new RuntimeException("Unexpected exception", e);
        }
    }
    
    // FUTURE - THINK ABOUT COMPLETELY REMOVING ANY READS/WRITES TO META NS FROM THE SERVER SIDE
    // clients can probably do it all
    
    public NamespaceProperties getNamespaceProperties(long namespace, NamespaceOptionsRetrievalMode retrievalMode) {
        try {
            NamespaceProperties    nsProperties;
            
            nsProperties = nsPropertiesMap.get(namespace);
            if (nsProperties != null) {
                return nsProperties;
            } else {
                switch (retrievalMode) {
                case LocalCheckOnly:
                    return null;
                case FetchRemotely:
                    nsProperties = nsOptionsClient.getNamespaceProperties(namespace, nsOptionsFetchTimeoutMillis);
                    if (nsProperties != null) {
                        nsPropertiesMap.put(namespace, nsProperties);
                        return nsProperties;
                    } else {
                        Log.warning(String.format("Namespace not found %x", namespace));
                        Log.warning(getNamespaceMetaDataReplicas(namespace));
                        throw new NamespaceNotCreatedException(Long.toHexString(namespace));
                    }
                default: throw new RuntimeException("Panic");
                }
            }
        } catch (TimeoutException te) {
            Log.warning("Failed to retrieve namespace due to timeout "+ String.format("%x", namespace));
            Log.warning(getNamespaceMetaDataReplicas(namespace));
            throw new NamespaceNotCreatedException(Long.toHexString(namespace), te);
        } catch (RetrievalException re) {
            Log.warning("Failed to retrieve namespace "+ String.format("%x", namespace));
            Log.warning(getNamespaceMetaDataReplicas(namespace));
            Log.warning(re.getDetailedFailureMessage());
            throw new NamespaceNotCreatedException(Long.toHexString(namespace), re);
        }
    }
    
    private String getNamespaceMetaDataReplicas(long ns) {
        SynchronousNamespacePerspective<String,String>  syncNSP;
        String  locations;
        
        syncNSP = session.getNamespace(Namespace.replicasName)
                         .openSyncPerspective(String.class, String.class);
        try {
            locations = syncNSP.get(Long.toString(ns));
            return locations;
        } catch (RetrievalException re2) {
            Log.warning(re2.getDetailedFailureMessage());
            Log.warning("Unexpected failure attempting to find replica locations "
                       +"during failed ns retrieval processing");
            return "Error";
        }
    }
    
    public void setNamespaceProperties(long namespace, NamespaceProperties nsProperties) {
        nsPropertiesMap.putIfAbsent(namespace, nsProperties);
    }
}
