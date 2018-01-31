package com.ms.silverking.cloud.skfs.management;

import org.kohsuke.args4j.Option;
//import com.ms.silverking.cloud.dht.management.Command;

enum Command {
	GetFromZK, LoadFromFile
} 

public class MetaUtilOptions {
	MetaUtilOptions(){
	}

    static final int dhtVersionUnspecified = -1;
    
    @Option(name="-g", usage="gridConfigName", required=false)
    String  gridConfigName;
    
    @Option(name="-d", usage="skfsConfigName", required=false)
    String  skfsConfigName;
    
    @Option(name="-v", usage="version", required=false)
    int     skfsVersion = dhtVersionUnspecified;
    
    @Option(name="-c", usage="command", required=true)
    Command command;
    
   @Option(name="-t", usage="targetFile", required=false)
    String  targetFile;
    
    @Option(name="-z", usage="zkEnsemble", required=true)
    String zkEnsemble;
}
