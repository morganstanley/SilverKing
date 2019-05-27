package com.ms.silverking.cloud.dht.meta;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.NamespaceCreationOptions;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.meta.Utils;
import com.ms.silverking.cloud.meta.VersionedDefinition;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParseException;
import com.ms.silverking.text.ObjectDefParser2;

/**
 * DHT configuration settings. 
 * (For use within the context of a single ZooKeeper ensemble -
 * thus specification of the ensemble is not necessary -
 * as opposed to ClientDHTConfiguration which specifies a
 * ZooKeeper ensemble.) 
 */
public class DHTConfiguration implements VersionedDefinition {
    private final String    ringName;
    private final int       port;
    private final String    passiveNodeHostGroups;
    private final NamespaceCreationOptions  nsCreationOptions;
    private final Map<String,String>    hostGroupToClassVarsMap;
    private final long      version;
    private final long      zkid;
    private final String	defaultClassVars;
    
    private static final Set<String> optionalFields;
    
    public static final DHTConfiguration  emptyTemplate = 
            new DHTConfiguration(null, 0, null, DHTConstants.defaultNamespaceCreationOptions, 
                    null, VersionedDefinition.NO_VERSION, VersionedDefinition.NO_VERSION,
                    null);    
    
    static {
        ImmutableSet.Builder<String> builder;
        
        builder = ImmutableSet.builder();
        builder.addAll(Utils.optionalVersionFieldSet);
        builder.add("nsCreationOptions");
        builder.add("zkid");
        builder.add("defaultClassVars");
        optionalFields = builder.build();
        
        ObjectDefParser2.addParser(emptyTemplate, FieldsRequirement.REQUIRE_ALL_NONOPTIONAL_FIELDS, optionalFields);        
    }
    
    public DHTConfiguration(String ringName, int port, String passiveNodeHostGroups, 
            NamespaceCreationOptions nsCreationOptions, Map<String,String> hostGroupToClassVarsMap, 
            long version, long zkid, String defaultClassVars) {
        this.ringName = ringName;
        this.port = port;
        this.passiveNodeHostGroups = passiveNodeHostGroups;
        this.nsCreationOptions = nsCreationOptions;
        this.hostGroupToClassVarsMap = hostGroupToClassVarsMap;
        this.version = version;
        this.zkid = zkid;
        this.defaultClassVars = defaultClassVars;
    }
    
    public static DHTConfiguration forPassiveNodes(String passiveNodeHostGroups) {
        return new DHTConfiguration(null, Integer.MIN_VALUE, passiveNodeHostGroups, null, null, 0, Long.MIN_VALUE, null);
    }
    
    public DHTConfiguration ringName(String ringName) {
        return new DHTConfiguration(ringName, port, passiveNodeHostGroups, nsCreationOptions, hostGroupToClassVarsMap, version, zkid, defaultClassVars);
    }
    
    public DHTConfiguration port(int port) {
        return new DHTConfiguration(ringName, port, passiveNodeHostGroups, nsCreationOptions, hostGroupToClassVarsMap, version, zkid, defaultClassVars);
    }
    
    public DHTConfiguration passiveNodeHostGroups(String passiveNodeHostGroups) {
        return new DHTConfiguration(ringName, port, passiveNodeHostGroups, nsCreationOptions, hostGroupToClassVarsMap, version, zkid, defaultClassVars);
    }
    
    public DHTConfiguration hostGroupToClassVarsMap(Map<String,String> hostGroupToClassVarsMap) {
        return new DHTConfiguration(ringName, port, passiveNodeHostGroups, nsCreationOptions, hostGroupToClassVarsMap, version, zkid, defaultClassVars);
    }    
    
    public DHTConfiguration version(long version) {
        return new DHTConfiguration(ringName, port, passiveNodeHostGroups, nsCreationOptions, hostGroupToClassVarsMap, version, zkid, defaultClassVars);
    }
    
    public DHTConfiguration zkid(long zkid) {
        return new DHTConfiguration(ringName, port, passiveNodeHostGroups, nsCreationOptions, hostGroupToClassVarsMap, version, zkid, defaultClassVars);
    }
    
    public DHTConfiguration defaultClassVars(String defaultClassVars) {
        return new DHTConfiguration(ringName, port, passiveNodeHostGroups, nsCreationOptions, hostGroupToClassVarsMap, version, zkid, defaultClassVars);
    }
    
    public String getRingName() {
        return ringName;
    }
    
    public int getPort() {
        return port;
    }
    
    public String getPassiveNodeHostGroups() {
        return passiveNodeHostGroups;
    }
    
    public Set<String> getPassiveNodeHostGroupsAsSet() {
    	return CollectionUtil.parseSet(passiveNodeHostGroups, ",");
    }
    
    public NamespaceCreationOptions getNSCreationOptions() {
        return nsCreationOptions;
    }
    
    public Map<String,String> getHostGroupToClassVarsMap() {
        return hostGroupToClassVarsMap;
    }
    
    public Set<String> getHostGroups() {
        return ImmutableSet.copyOf(hostGroupToClassVarsMap.keySet());
    }
    
    @Override
    public long getVersion() {
        return version;
    }
    
    public long getZKID() {
        return zkid;
    }
    
    public String getDefaultClassVars() {
    	return defaultClassVars;
    }
    
    public static DHTConfiguration parse(String def, long version) {
        DHTConfiguration    instance;
        
        try {
        	instance = ObjectDefParser2.parse(DHTConfiguration.class, def);
        } catch (ObjectDefParseException odpe) {
        	// FIXME - below is temporary to allow interaction with old instances
        	instance = ObjectDefParser2.parse(DHTConfiguration.class, def.replaceAll("passiveNodeHostGroups", "passiveNodes"));
        }
        return instance.version(version);
    }
    
    public static DHTConfiguration parse(String def, long version, long zkid) {
        DHTConfiguration    instance;
        
        instance = parse(def, version);
        return instance.zkid(version);
    }
    
    @Override
    public String toString() {
        return ObjectDefParser2.objectToString(this);
    }

	public boolean hasPassiveNodeHostGroups() {
		return passiveNodeHostGroups != null && passiveNodeHostGroups.trim().length() > 0;
	}
}
