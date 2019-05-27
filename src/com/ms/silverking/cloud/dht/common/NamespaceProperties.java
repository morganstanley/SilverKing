package com.ms.silverking.cloud.dht.common;

import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.text.ObjectDefParser2;

/**
 * Internal metadata for a Namespace.  
 */
public class NamespaceProperties {
    private final NamespaceOptions  options;
    private final String            parent;
    private final long              minVersion;
    private final long              creationTime;
    
    private static final NamespaceProperties    templateProperties = new NamespaceProperties();
    private static final Set<String>			excludedFields = ImmutableSet.of("creationTime");

    static {
        ObjectDefParser2.addParserWithExclusions(templateProperties, excludedFields);
    }
    
    public static void initParser() { 
    }
    
    
    public NamespaceProperties(NamespaceOptions options, String parent, long minVersion, long creationTime) {
        this.options = options;
        this.parent = parent;
        this.minVersion = minVersion;
        this.creationTime = creationTime;
    }
    
    public NamespaceProperties(NamespaceOptions options, String parent, long minVersion) {
    	this(options, parent, minVersion, 0);
    }
    
    public NamespaceProperties(NamespaceOptions options) {
        this(options, null, Long.MIN_VALUE);
    }
    
    private NamespaceProperties() {
        this(DHTConstants.defaultNamespaceOptions);
    }
    
    public NamespaceOptions getOptions() {
        return options;
    }
    
    public String getParent() {
        return parent;
    }
    
    public long getMinVersion() {
        return minVersion;
    }
    
    public long getCreationTime() {
        return creationTime;
    }
    
    public NamespaceProperties creationTime(long creationTime) {
    	return new NamespaceProperties(options, parent, minVersion, creationTime);
    }
    
    public NamespaceProperties options(NamespaceOptions options) {
    	return new NamespaceProperties(options, parent, minVersion, creationTime);
    }
    
    @Override
    public int hashCode() {
        return options.hashCode() ^ (parent == null ? 0 : parent.hashCode());
    }
    
    @Override
    public boolean equals(Object o) {
        NamespaceProperties oProperties;
        
        oProperties = (NamespaceProperties)o;
        return this.options.equals(oProperties.options) && Objects.equals(this.parent, oProperties.parent);
    }
    
    public void debugEquals(Object o) {
        NamespaceProperties oProperties;
        
        oProperties = (NamespaceProperties)o;
        this.options.debugEquality(oProperties.options);
    }
    
    @Override
    public String toString() {
        return ObjectDefParser2.objectToString(this);
    }
    
    public static NamespaceProperties parse(String def, long creationTime) {
        return ((NamespaceProperties)ObjectDefParser2.parse(NamespaceProperties.class, def)).creationTime(creationTime);
    }
}
