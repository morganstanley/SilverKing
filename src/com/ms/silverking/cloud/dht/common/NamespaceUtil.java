package com.ms.silverking.cloud.dht.common;

import java.util.HashMap;
import java.util.Map;

import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.InvalidationOptions;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespacePerspectiveOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.StorageType;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.client.AbsMillisVersionProvider;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.KeyDigestType;
import com.ms.silverking.cloud.dht.client.impl.NamespaceCreator;
import com.ms.silverking.cloud.dht.client.impl.SimpleNamespaceCreator;

public class NamespaceUtil {
    private static final NamespaceCreator   creator = new SimpleNamespaceCreator();
    
    public static final String metaInfoNamespaceName = "__DHT_Meta__";
    public static final Namespace   metaInfoNamespace = creator.createNamespace(metaInfoNamespaceName);
    public static final PutOptions  metaNSDefaultPutOptions = new PutOptions(DHTConstants.standardTimeoutController, 
            null, Compression.NONE, ChecksumType.MD5, false, 0L, null); 
    public static final InvalidationOptions  metaNSDefaultInvalidationOptions = OptionsHelper.newInvalidationOptions(DHTConstants.standardTimeoutController, 0L, null); 
    public static final NamespaceOptions    metaInfoNamespaceOptions 
        = OptionsHelper.newNamespaceOptions(StorageType.FILE, ConsistencyProtocol.TWO_PHASE_COMMIT, 
                               NamespaceVersionMode.SINGLE_VERSION, 
                               metaNSDefaultPutOptions, metaNSDefaultInvalidationOptions,
                               DHTConstants.standardGetOptions, DHTConstants.standardWaitOptions).asWriteOnce();
                               // meta info ns must be write once currently because convergence only supports
                               // SINGLE_VERSION currently
                               // This implies that namespace options of a namespace can never change.
                               // That's probably a sensible invariant to require anyway.
    public static final NamespaceProperties metaInfoNamespaceProperties 
        = new NamespaceProperties(metaInfoNamespaceOptions);
    public static final Map<Long,NamespaceProperties>	systemNamespaceProperties;
    
    static {
    	SimpleNamespaceCreator	nsCreator;
    	
    	nsCreator = new SimpleNamespaceCreator();
    	systemNamespaceProperties = new HashMap<>();
    	systemNamespaceProperties.put(nsCreator.createNamespace(com.ms.silverking.cloud.dht.client.Namespace.nodeName).contextAsLong(), new NamespaceProperties(DHTConstants.dynamicNamespaceOptions));
    	systemNamespaceProperties.put(nsCreator.createNamespace(com.ms.silverking.cloud.dht.client.Namespace.replicasName).contextAsLong(), new NamespaceProperties(DHTConstants.dynamicNamespaceOptions));
    }

    public static final NamespacePerspectiveOptions<String,String>    metaNSPOptions = 
            new NamespacePerspectiveOptions<>(String.class, String.class, 
                    KeyDigestType.MD5, 
                    DHTConstants.standardPutOptions,//metaNSDefaultPutOptions,
                    DHTConstants.standardInvalidationOptions,
                    OptionsHelper.newGetOptions(RetrievalType.VALUE, VersionConstraint.greatest),
                    DHTConstants.standardWaitOptions,
                    new AbsMillisVersionProvider(SystemTimeUtil.systemTimeSource), null);
            
    public static long nameToLong(String namespace) {
        return new SimpleNamespaceCreator().createNamespace(namespace).contextAsLong();
    }
            
    public static String nameToHexString(String namespace) {
        return Long.toHexString(nameToLong(namespace));
    }
}
