package com.ms.silverking.cloud.dht.client;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.SessionOptions;
import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.net.HostAndPort;
import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;

/**
 * Client configuration settings for a single DHT.
 */
public class ClientDHTConfiguration implements ClientDHTConfigurationProvider {
	private final String	       name;
    private final int              port;
	private final AddrAndPort[]    zkLocs;
	
	public static ClientDHTConfiguration	embeddedKVS = new ClientDHTConfiguration(SessionOptions.EMBEDDED_KVS, new AddrAndPort[0]);
	
	private static final int   portInZKOnly = Integer.MIN_VALUE;
	
	public static final String	nameVar = "GC_SK_NAME";
    public static final String	portVar = "GC_SK_PORT";
	public static final String	zkLocVar = "GC_SK_ZK_LOC";
	
    private static final Set<String> optionalFields;
    
    public static final ClientDHTConfiguration  emptyTemplate = 
            new ClientDHTConfiguration("__dummy_name__", 1, "localhost:0");
    private static final Class[]	constructorFieldClasses = new Class[]{String.class, int.class, String.class};
    private static final String[]	constructorFieldNames = new String[]{"name", "port", "zkLocs"};
    
    static {
        ImmutableSet.Builder<String> builder;
        
        builder = ImmutableSet.builder();
        optionalFields = builder.build();
        
        ObjectDefParser2.addParser(emptyTemplate, FieldsRequirement.REQUIRE_ALL_NONOPTIONAL_FIELDS, optionalFields, constructorFieldClasses, constructorFieldNames);        
    }
	
    public ClientDHTConfiguration(String dhtName, int dhtPort, String zkLocs) {
        this(dhtName, dhtPort, HostAndPort.parseMultiple(zkLocs));
    }
    
    public ClientDHTConfiguration(String dhtName, int dhtPort, AddrAndPort[] zkLocs) {
    	Preconditions.checkNotNull(dhtName, "dhtName must be non-null");
    	Preconditions.checkArgument(dhtPort > 0 || dhtPort == portInZKOnly, "dhtPort must be > 0. Found: ", dhtPort);
    	Preconditions.checkNotNull(zkLocs, "zkLocs must be non-null");
        this.name = dhtName;
        this.port = dhtPort;
        this.zkLocs = zkLocs;
    }
    
    public ClientDHTConfiguration(String dhtName, AddrAndPort[] zkLocs) {
        this(dhtName, portInZKOnly, zkLocs);
    }
    
    public ClientDHTConfiguration(String dhtName, String zkLocs) {
        this(dhtName, HostAndPort.parseMultiple(zkLocs));
    }
    
	public static ClientDHTConfiguration create(Map<String,String> envMap) {
		return new ClientDHTConfiguration(envMap.get(nameVar), 
		        envMap.get(portVar) == null ? portInZKOnly : Integer.parseInt(envMap.get(portVar)), 
				HostAndPort.parseMultiple(envMap.get(zkLocVar)));
	}
	
	public String getName() {
		return name;
	}
	
    public int getPort() {
        return port;
    }
    
	public AddrAndPort[] getZkLocs() {
		return zkLocs;
	}
	
    @Override
    public ClientDHTConfiguration getClientDHTConfiguration() {
        return this;
    }
	
	public String toString() {
		StringBuilder sb;
		boolean       first;
		
		sb = new StringBuilder();
		sb.append(name);
		sb.append(':');
        first = true;
        for (AddrAndPort zkLoc : zkLocs) {
            if (!first) {
                sb.append(AddrAndPort.multipleDefDelimiter);
            } else {
                first = false;
            }
            sb.append(zkLoc.toString());
        }
		return sb.toString();
	}

    public boolean hasPort() {
        return port != portInZKOnly;
    }
    
    @Override
    public int hashCode() {
        int hashCode;
        
        hashCode = name.hashCode() ^ port;
        for (AddrAndPort member : zkLocs) {
            hashCode = hashCode ^ member.hashCode();
        }
        return hashCode;
    }
    
    @Override
    public boolean equals(Object other) {
        ClientDHTConfiguration  otherCDC;
        
        otherCDC = (ClientDHTConfiguration)other;
        if (!name.equals(otherCDC.name)) {
            return false;
        } else if (port != otherCDC.port) {
            return false;
        } else {
            if (zkLocs.length != otherCDC.zkLocs.length) {
                return false;
            } else {
                // Note that this implementation is currently order-sensitive
                for (int i = 0; i < zkLocs.length; i++) {
                    if (!zkLocs[i].equals(otherCDC.zkLocs[i])) {
                        return false;
                    }
                }
                return true;
            }
        }
    }
    
    public static ClientDHTConfiguration parse(String def) {
        ClientDHTConfiguration	instance;
        
        instance = ObjectDefParser2.parse(ClientDHTConfiguration.class, def);
        return instance;
    }
}
