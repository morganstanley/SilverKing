package com.ms.silverking.cloud.toporing.meta;

import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;


public class NamedRingConfiguration {
    private final String            ringName;
    private final RingConfiguration ringConfig;
    
    //public static final char    delimiter = RingConfiguration.delimiter;
    
    public static final NamedRingConfiguration  emptyTemplate = 
            new NamedRingConfiguration(null, RingConfiguration.emptyTemplate);

    static {
        ObjectDefParser2.addParser(emptyTemplate, FieldsRequirement.REQUIRE_ALL_FIELDS);
    }
    
    public NamedRingConfiguration(String ringName, RingConfiguration ringConfig) {
        this.ringName = ringName;
        this.ringConfig = ringConfig;
    }
    
    public NamedRingConfiguration ringName(String ringName) {
    	return new NamedRingConfiguration(ringName, ringConfig);
    }
    
    public String getRingName() {
        return ringName;
    }
    
    public RingConfiguration getRingConfiguration() {
        return ringConfig;
    }

    /*
    public static NamedRingConfiguration parse(String def, long version) {
        int index;
        
        index = def.indexOf(delimiter);
        if (index < 0) {
            throw new RuntimeException("bad NamedRingConfiguration def: "+ def);
        } else {
            RingConfiguration   ringConfig;
            String              ringName;
            
            ringName = def.substring(0, index);
            ringConfig = RingConfiguration.parse(def.substring(index + 1), version);
            return new NamedRingConfiguration(ringName, ringConfig);
        }
    }
    */
    
    public static NamedRingConfiguration parse(String def) {
        return ObjectDefParser2.parse(NamedRingConfiguration.class, def);
    }
    
    @Override
    public String toString() {
        return ObjectDefParser2.objectToString(this);
    }    
}
