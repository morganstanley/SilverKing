package com.ms.silverking.cloud.toporing;

import org.kohsuke.args4j.Option;

public class MasterModeDependencyWatcherOptions {
    MasterModeDependencyWatcherOptions() {
    }
    
    @Option(name="-g", usage="gridConfig", required=true)
    String  gridConfig;
    
    @Option(name="-i", usage="watchIntervalSeconds", required=false)
    int     watchIntervalSeconds = 15;
    
    @Option(name="-c", usage="consecutiveUpdateGuardSeconds", required=false)
    int     consecutiveUpdateGuardSeconds = 60;    
}
