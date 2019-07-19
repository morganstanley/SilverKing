package com.ms.silverking.cloud.dht.meta;

import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;


public class NamedDHTConfiguration {
    private final String            dhtName;
    private final DHTConfiguration  dhtConfig;
    
    public static final NamedDHTConfiguration  emptyTemplate = 
            new NamedDHTConfiguration(null, DHTConfiguration.emptyTemplate);
    
    static {
        ObjectDefParser2.addParser(emptyTemplate, FieldsRequirement.REQUIRE_ALL_FIELDS);
    }
    
    public NamedDHTConfiguration(String dhtName, DHTConfiguration dhtConfig) {
        this.dhtName = dhtName;
        this.dhtConfig = dhtConfig;
    }
    
    public String getDHTName() {
        return dhtName;
    }

    public DHTConfiguration getDHTConfig() {
        return dhtConfig;
    }
    
    /*
    public static NamedDHTConfiguration parse(String def, long version) {
        DHTConfiguration    dhtConfig;
        String              dhtName;
        int                 index;
        
        index = def.indexOf(delimiter);
        if (index < 0) {
            dhtName = def;
            dhtConfig = null;
        } else {
            dhtName = def.substring(0, index);
            dhtConfig = DHTConfiguration.parse(def.substring(index + 1), version);
        }
        return new NamedDHTConfiguration(dhtName, dhtConfig);
    }    
    */

    public static NamedDHTConfiguration parse(String def) {
        return ObjectDefParser2.parse(NamedDHTConfiguration.class, def);
    }
    
    @Override
    public String toString() {
        return ObjectDefParser2.objectToString(this);
    }    
}
