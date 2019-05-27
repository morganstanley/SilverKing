package com.ms.silverking.cloud.meta;

import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;

/**
 * Groups cloud configuration settings. Neither named nor stored in zk.
 */
public class CloudConfiguration {
    private final String    topologyName;
    private final String    exclusionSpecsName;
    private final String    hostGroupTableName;
    
    public static final CloudConfiguration emptyTemplate = new CloudConfiguration(null, null, null);

    static {
        ObjectDefParser2.addParser(emptyTemplate, FieldsRequirement.REQUIRE_ALL_FIELDS);
    }
    
    public CloudConfiguration(String topologyName, String exclusionSpecsName, 
                              String hostGroupTableName) {
        this.topologyName = topologyName;
        this.exclusionSpecsName = exclusionSpecsName;
        this.hostGroupTableName = hostGroupTableName;
    }
    
    public CloudConfiguration topologyName(String topologyName) {
        return new CloudConfiguration(topologyName, exclusionSpecsName, hostGroupTableName);
    }
    
    public CloudConfiguration exclusionSpecsName(String exclusionSpecsName) {
        return new CloudConfiguration(topologyName, exclusionSpecsName, hostGroupTableName);
    }
    
    public CloudConfiguration hostGroupTableName(String hostGroupTableName) {
        return new CloudConfiguration(topologyName, exclusionSpecsName, hostGroupTableName);
    }
    
    public String getTopologyName() {
        return topologyName;
    }

    public String getExclusionSpecsName() {
        return exclusionSpecsName;
    }
    
    public String getHostGroupTableName() {
        return hostGroupTableName;
    }
    
    public static CloudConfiguration parse(String def) {
        return ObjectDefParser2.parse(CloudConfiguration.class, def);
    }
    
    @Override
    public String toString() {
        return ObjectDefParser2.objectToString(this);
    }    
}
