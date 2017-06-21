package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import org.kohsuke.args4j.Option;

public class DHTRingMasterOptions {
    @Option(name="-g", usage="GridConfig", required=true)
    public String gridConfig;
    @Option(name="-i", usage="intervalSeconds", required=false)
    public int  watchIntervalSeconds = 10;
    @Option(name="-m", usage="Mode", required=false)
    public Mode mode = Mode.Manual;
}
