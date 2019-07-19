package com.ms.silverking.pssh;

import org.kohsuke.args4j.Option;

public class TwoLevelParallelSSHOptions {
    TwoLevelParallelSSHOptions() {
    }
    
    private static final int    defaultWorkerThreads = 20;
    private static final int    defaultTimeoutSeconds = 60;
    private static final int    defaultMaxAttempts = 1;
    
    @Option(name="-c", usage="command", required=true)
    String  command;
    
    @Option(name="-h", usage="hostsFile[:optionalGroup]", required=true)
    String  hostsFile_optionalGroup;
    
    @Option(name="-e", usage="exclusionsFile", required=false)
    String exclusionsFile;
    
    @Option(name="-w", usage="workerCandidatesFile", required=false)
    String workerCandidatesFile;    
    
    @Option(name="-n", usage="numWorkerThreads", required=false)
    int     numWorkerThreads = defaultWorkerThreads;
    
    @Option(name="-t", usage="timeoutSeconds", required=false)
    int     timeoutSeconds = defaultTimeoutSeconds;
    
    @Option(name="-m", usage="maxAttempts", required=false)
    int     maxAttempts = defaultMaxAttempts;    
}
