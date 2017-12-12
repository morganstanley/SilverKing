package com.ms.silverking.cloud.dht.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ms.silverking.cloud.dht.NamespaceCreationOptions;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.SessionOptions;
import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.NamespaceCreationException;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.SessionEstablishmentTimeoutController;
import com.ms.silverking.cloud.dht.client.StoredValue;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.impl.SimpleNamespaceCreator;
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceNotCreatedException;
import com.ms.silverking.cloud.dht.meta.DHTMetaWatcher;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.collection.HashedListMap;
import com.ms.silverking.log.Log;
import com.ms.silverking.thread.lwt.LWTThreadUtil;

public class NamespaceOptionsClient {
    private final ZooKeeperConfig   zkConfig;
    private final String            dhtName;
    private final SynchronousNamespacePerspective<String,String>    syncNSP;
    private NamespaceCreationOptions  nsCreationOptions;
    private DHTMetaWatcher  dhtMetaWatcher;
    private final SessionEstablishmentTimeoutController	seTimeoutController;
    private final Map<Long,NamespaceOptions>	systemNamespaceOptions;
    
    private static final int    dhtMetaWatcherIntervalMillis = 60 * 60 * 1000;
    
    private static final boolean    debug = false;
    
    private static final ConcurrentMap<String,NamespaceCreationOptions> nsCreationOptionsMap;
    
    static {
        nsCreationOptionsMap = new ConcurrentHashMap<>();
    }
    
    private NamespaceOptionsClient(DHTSession session, ZooKeeperConfig zkConfig, String dhtName, 
    		SessionEstablishmentTimeoutController seTimeoutController) {
    	SimpleNamespaceCreator	nsCreator;
    	
        syncNSP = session.openSyncNamespacePerspective(NamespaceUtil.metaInfoNamespaceName, NamespaceUtil.metaNSPOptions);
        this.zkConfig = zkConfig;
        this.dhtName = dhtName;
        this.seTimeoutController = seTimeoutController;
        systemNamespaceOptions = new HashMap<>();
        nsCreator = new SimpleNamespaceCreator();
        systemNamespaceOptions.put(nsCreator.createNamespace(Namespace.systemName).contextAsLong(), DHTConstants.dynamicNamespaceOptions);
        systemNamespaceOptions.put(nsCreator.createNamespace(Namespace.nodeName).contextAsLong(), DHTConstants.dynamicNamespaceOptions);
        systemNamespaceOptions.put(nsCreator.createNamespace(Namespace.replicasName).contextAsLong(), DHTConstants.dynamicNamespaceOptions);
    }
    
    public NamespaceOptionsClient(DHTSession session) {
        this(session, null, null, SessionOptions.getDefaultTimeoutController());
    }
    
    public NamespaceOptionsClient(DHTSession session, ClientDHTConfiguration dhtConfig, 
    							SessionEstablishmentTimeoutController seTimeoutController) {
        this(session, dhtConfig.getZKConfig(), dhtConfig.getName(), seTimeoutController);
    }

    public void createNamespace(String namespace, NamespaceProperties nsProperties) throws NamespaceCreationException {
        ensureNSCreationOptionsSet();
        if (debug) {
            System.out.printf("canBeExplicitlyCreated %s\n", nsCreationOptions.canBeExplicitlyCreated(namespace));
        }
        if (nsCreationOptions.canBeExplicitlyCreated(namespace)) {
            storeNamespaceProperties(NamespaceUtil.nameToLong(namespace), nsProperties);
        } else {
            throw new NamespaceCreationException("Namespace creation not allowed for "+ namespace);
        }
    }
    
    public void storeNamespaceProperties(long namespace, NamespaceProperties nsProperties) throws NamespaceCreationException {
    	boolean	retry;
    	
    	retry = false;
    	do {
	        NamespaceProperties existingProperties;
	        
	        try {
	            existingProperties = getNamespaceProperties(namespace, seTimeoutController.getMaxRelativeTimeoutMillis(null));
	        } catch (TimeoutException te) {
	            Log.warning("Failed to store namespace due to timeout "+ String.format("%x", namespace));
	            throw new NamespaceCreationException(Long.toHexString(namespace), te);
	        } catch (RetrievalException re) {
	        	Log.warning(re.getDetailedFailureMessage());
	            throw new NamespaceCreationException("RetrievalException during first property check", re);
	        }
	        if (existingProperties != null) {
	            if (!existingProperties.equals(nsProperties)) {
	                Log.warning("existingProperties", existingProperties);
	                Log.warning("nsProperties", nsProperties);
	                throw new NamespaceCreationException("Namespace already created with incompatible properties");
	            } else {
	                // Already created with the same options. No further action required.
	            }
	        } else {
	            try {
	                if (debug) {
	                    System.out.printf("storeNamespaceProperties(%x, %s)\n", namespace, nsProperties);
	                }
	                syncNSP.put(getOptionsKey(namespace), nsProperties.toString());
	                if (debug) {
	                    System.out.println("Done storeNamespaceOptions");
	                }
	            } catch (PutException pe) {
	                // If a simultaneous put is detected, then we must check for
	                // consistency among the created namespaces.
	                // For other errors, we try this also on the
	                try {
	                    boolean optionsMatch;
	                    
	                    try {
	                        optionsMatch = verifyNamespaceProperties(namespace, nsProperties);
	                        if (!optionsMatch) {
	                            throw new NamespaceCreationException("Namespace already created with incompatible properties");
	                        }
	                    } catch (NamespaceNotCreatedException nnce) {
	                    	// Should not be possible any more, but leave old retry in for now.
	                    	retry = true;
	                    } catch (RuntimeException re) {
	                        Log.logErrorWarning(re);
	                        Log.warning(pe.getDetailedFailureMessage());
	                        Log.logErrorWarning(pe, "Couldn't store options due to exception");
	                    }
	                } catch (RetrievalException re) {
	                    Log.warning("storeNamespaceProperties failing");
	                    Log.warning("PutException");
	                    pe.printStackTrace();
	                    Log.warning("RetrievalException");
	                    re.printStackTrace();
	                    throw new NamespaceCreationException(re);
	                }
	            }
	        }
    	} while (retry);
    }

    private boolean verifyNamespaceProperties(long namespace, NamespaceProperties nsProperties) throws RetrievalException {
        NamespaceOptions    existingProperties;
        
        if (debug) {
            System.out.printf("verifyNamespaceProperties(%x, %s)\n", namespace, nsProperties);
        }
        existingProperties = getNamespaceOptions(namespace);
        if (debug) {
            System.out.println("Done verifyNamespaceProperties");
        }
        if (existingProperties == null) {
        	throw new NamespaceNotCreatedException("No existing properties found");
        }
        return existingProperties.equals(nsProperties);
    }
    
    private boolean verifyNamespaceOptions(long namespace, NamespaceOptions nsOptions) throws RetrievalException {
        NamespaceOptions    existingOptions;
        
        if (debug) {
            System.out.printf("verifyNamespaceOptions(%x, %s)\n", namespace, nsOptions);
        }
        existingOptions = getNamespaceOptions(namespace);
        if (debug) {
            System.out.println("Done verifyNamespaceOptions");
        }
        return existingOptions.equals(nsOptions);
    }
    
    private String getOptionsKey(long namespace) {
        return Long.toString(namespace);
    }
    
    public NamespaceProperties getNamespaceProperties(long namespace, long relTimeoutMillis) throws RetrievalException, TimeoutException {
        if (namespace != NamespaceUtil.metaInfoNamespace.contextAsLong()) {
        	NamespaceProperties	nsProperties;
        	
        	nsProperties = NamespaceUtil.systemNamespaceProperties.get(namespace);
        	if (nsProperties != null) {
        		return nsProperties;
        	} else {
	        	StoredValue<String>	storedDef;
	            
	            storedDef = null;
	            LWTThreadUtil.setBlocked();
	            try {
	                ActiveOptionsRequest    request;
	                boolean                 requestsOutstanding;
	                
	                if (debug) {
	                    System.out.printf("getNamespaceProperties(%x) timeout %d\n", namespace, relTimeoutMillis);
	                }
	                requestsOutstanding = false;
	                request = new ActiveOptionsRequest(namespace, relTimeoutMillis);
	                activeOptionsRequestLock.lock();
	                try {
	                    List<ActiveOptionsRequest>    requestList;
	                    
	                    requestList = activeOptionsRequests.getList(namespace);
	                    requestsOutstanding = requestList.size() > 0;
	                    requestList.add(request);
	                } finally {
	                    activeOptionsRequestLock.unlock();
	                }
	                if (debug) {
	                    System.out.printf("requestsOutstanding %s\n", requestsOutstanding);
	                }
	                if (!requestsOutstanding) {
	                    if (debug) {
	                        System.out.printf("syncNSP.retrieve()\n");
	                    }
	                	storedDef = syncNSP.retrieve(getOptionsKey(namespace), syncNSP.getOptions().getDefaultGetOptions().retrievalType(RetrievalType.VALUE_AND_META_DATA));
	                    if (debug) {
	                        System.out.printf("syncNSP.retrieve() complete %s\n", storedDef);
	                    }
	                    activeOptionsRequestLock.lock();
	                    try {
	                        List<ActiveOptionsRequest>  requestList;
	                        
	                        requestList = activeOptionsRequests.getList(namespace);
	                        for (int i = 1; i < requestList.size(); i++) {
	                            requestList.get(i).setComplete(storedDef);
	                        }
	                        requestList.clear();
	                    } finally {
	                        activeOptionsRequestLock.unlock();
	                    }
	                } else {
	                    if (debug) {
	                        System.out.printf("request.waitForCompletion()\n");
	                    }
	                    storedDef = request.waitForCompletion();
	                    if (debug) {
	                        System.out.printf("request.waitForCompletion() complete %s\n", storedDef);
	                    }
	                    if (storedDef == null) {
	                    	storedDef = syncNSP.retrieve(getOptionsKey(namespace), syncNSP.getOptions().getDefaultGetOptions().retrievalType(RetrievalType.VALUE_AND_META_DATA));
	                    }
	                }
	            } finally {
	                LWTThreadUtil.setNonBlocked();
	            }
	            if (debug) {
	                System.out.printf("getNamespaceProperties storedDef %s\n", storedDef);
	            }
	            if (storedDef != null) {
	                return NamespaceProperties.parse(storedDef.getValue(), storedDef.getCreationTime().inNanos());
	            } else {
	                return null;
	            }
        	}
        } else {
            return NamespaceUtil.metaInfoNamespaceProperties;
        }
    }    
    
    public NamespaceOptions getNamespaceOptions(long namespace) throws RetrievalException {
        if (namespace != NamespaceUtil.metaInfoNamespace.contextAsLong()) {
        	NamespaceOptions	nsOptions;
        	
        	nsOptions = systemNamespaceOptions.get(namespace);
        	if (nsOptions != null) {
        		return nsOptions;
        	} else {
	            String  def;
	            
	            def = null;
	            LWTThreadUtil.setBlocked();
	            try {
	                if (debug) {
	                    System.out.printf("getNamespaceOptions(%x)\n", namespace);
	                }
	                def = syncNSP.get(getOptionsKey(namespace));
	            } finally {
	                LWTThreadUtil.setNonBlocked();
	            }
	            if (debug) {
	                System.out.printf("getNamespaceOptions def %s\n", def);
	            }
	            if (def != null) {
	                return NamespaceOptions.parse(def);
	            } else {
	                return null;
	            }
        	}
        } else {
            return NamespaceUtil.metaInfoNamespaceOptions;
        }
    }
    
    public NamespaceProperties getNamespaceProperties(String namespace) throws RetrievalException, TimeoutException {
        long    context;
        NamespaceProperties    nsProperties;
        
        context = NamespaceUtil.nameToLong(namespace);
        nsProperties = getNamespaceProperties(context, seTimeoutController.getMaxRelativeTimeoutMillis(null));
        if (nsProperties == null) {
            ensureNSCreationOptionsSet();
            if (debug) {
                System.out.printf("canBeAutoCreated %s\n", nsCreationOptions.canBeAutoCreated(namespace));
            }
            if (nsCreationOptions.canBeAutoCreated(namespace)) {
                try {
                    storeNamespaceProperties(context, new NamespaceProperties(nsCreationOptions.getDefaultNamespaceOptions()));
                } catch (NamespaceCreationException nce) {
                    throw new RuntimeException("Failure autocreating namespace", nce);
                }
                nsProperties = getNamespaceProperties(context, seTimeoutController.getMaxRelativeTimeoutMillis(null));
                if (nsProperties == null) {
                    throw new RuntimeException("Failure autocreating namespace. null nsProperties");
                }
            }
        }
        return nsProperties;
    }
    
    private void ensureNSCreationOptionsSet() {
        if (debug) {
            Log.warning("ensureNSCreationOptionsSet()");
            Thread.dumpStack();
        }
        synchronized (this) {
            if (nsCreationOptions == null) {
                nsCreationOptions = nsCreationOptionsMap.get(dhtName);
            }
            if (nsCreationOptions == null) {
                if (dhtMetaWatcher == null) {
                    assert nsCreationOptions == null;
                    try {
                        if (debug) {
                            Log.warning("ensureNSCreationOptionsSet() calling dhtMetaWatcher");
                        }
                        dhtMetaWatcher = new DHTMetaWatcher(zkConfig, dhtName, dhtMetaWatcherIntervalMillis, debug);
                        dhtMetaWatcher.waitForDHTConfiguration();
                        nsCreationOptions = dhtMetaWatcher.getDHTConfiguration().getNSCreationOptions();
                        nsCreationOptionsMap.put(dhtName, nsCreationOptions);
                        if (debug) {
                            Log.warning("nsCreationOptions: ", nsCreationOptions);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }
    
    public NamespaceCreationOptions getNamespaceCreationOptions() {
        ensureNSCreationOptionsSet();
        return nsCreationOptions;
    }
    
    /////
    
    private final Lock  activeOptionsRequestLock = new ReentrantLock();
    private final HashedListMap<Long,ActiveOptionsRequest>    activeOptionsRequests = new HashedListMap<>(true);
    private static final int   activeOptionsRequestTimeoutMillis = 2 * 60 * 1000;
    
    private static class ActiveOptionsRequest {
        private final long  ns; // currently stored solely for debugging/logging
        private final long	absTimeoutMillis;
        private StoredValue<String>	storedDef;
        
        ActiveOptionsRequest(long ns, long relTimeoutMillis) {
            this.ns = ns;
            this.absTimeoutMillis = SystemTimeUtil.systemTimeSource.absTimeMillis() + relTimeoutMillis;
        }
        
        StoredValue<String> waitForCompletion() throws TimeoutException {
            synchronized (this) {
                while (storedDef == null) {
                	long	relDeadlineMillis;
                	long	timeToWaitMillis;
                	
                	relDeadlineMillis = Math.max(0, absTimeoutMillis - SystemTimeUtil.systemTimeSource.absTimeMillis());
                    try {
                    	timeToWaitMillis = Math.min(activeOptionsRequestTimeoutMillis, relDeadlineMillis); 
                    	if (debug) {
                    		System.out.printf("timeToWaitMillis %d\n", timeToWaitMillis);
                    	}
                    	if (timeToWaitMillis == 0) {
                    		throw new TimeoutException(Long.toString(absTimeoutMillis));
                    	}
                        this.wait(timeToWaitMillis);
                    	if (debug) {
                    		System.out.printf("out of wait\n");
                    	}
                    } catch (InterruptedException e) {
                    	e.printStackTrace();
                    }
                }
                if (storedDef == null) {
                	throw new RuntimeException("ActiveOptionsRequest.waitForCompletion() timed out");
                }
            }
        	if (debug) {
        		System.out.printf("storedDef %d", storedDef);
        	}
            return storedDef;
        }
        
        void setComplete(StoredValue<String> storedDef) {
            synchronized (this) {
                if (this.storedDef != null) {
                    Log.warning(storedDef);
                    Log.warning(this.storedDef);
                    Log.warning("Unexpected multiple completion for ActiveOptionsRequest");
                } else {
                    this.storedDef = storedDef;
                }
                this.notifyAll();
            }
        }
    }
}
