package com.ms.silverking.cloud.dht.meta;

import org.kohsuke.args4j.Option;

public class DHTRingMasterOptions {
    @Option(name="-g", usage="GridConfig", required=true)
    public String gridConfig;
    @Option(name="-i", usage="intervalSeconds", required=false)
    public int  watchIntervalSeconds = 10;
}
