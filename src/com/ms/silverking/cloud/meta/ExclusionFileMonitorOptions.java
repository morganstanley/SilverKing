package com.ms.silverking.cloud.meta;

import org.kohsuke.args4j.Option;

public class ExclusionFileMonitorOptions {
    @Option(name="-g", usage="gridConfig", required=true)
    public String gridConfig;
    @Option(name="-i", usage="intervalSeconds", required=false)
    public int  watchIntervalSeconds = 10;
    @Option(name="-f", usage="exclusionFile", required=true)
    public String exclusionFile;
    @Option(name="-e", usage="exclusionListName", required=false)
    public String exclusionListName;
}
