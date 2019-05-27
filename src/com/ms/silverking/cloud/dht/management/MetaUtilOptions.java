package com.ms.silverking.cloud.dht.management;

import org.kohsuke.args4j.Option;

public class MetaUtilOptions {
    MetaUtilOptions() {
    }
    
    public static final int dhtVersionUnspecified = -1;
    
    @Option(name="-d", usage="dhtName", required=true)
    String  dhtName;
    
    @Option(name="-v", usage="")
    int     dhtVersion = dhtVersionUnspecified;
    
    @Option(name="-c", usage="command", required=true)
    Command command;
    
    @Option(name="-f", usage="filterOption", required=false)
    FilterOption    filterOption;
    
    @Option(name="-t", usage="filterOption", required=false)
    String  targetFile;
    
    @Option(name="-h", usage="hostGroups")
    String  hostGroups;
    
    @Option(name="-z", usage="zkEnsemble", required=true)
    String zkEnsemble;
    
    @Option(name="-e", usage="exclusionsFile", required=false)
    String exclusionsFile;
    
    @Option(name="-w", usage="workersFile", required=false)
    String workersFile;    
}
