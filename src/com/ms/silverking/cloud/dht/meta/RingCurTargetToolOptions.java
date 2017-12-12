package com.ms.silverking.cloud.dht.meta;

import org.kohsuke.args4j.Option;

public class RingCurTargetToolOptions {
    @Option(name="-g", usage="GridConfig", required=true)
    public String gridConfig;
    @Option(name="-m", usage="mode {Read,Write}", required=true)
    public RingCurTargetTool.Mode	mode;
    @Option(name="-o", usage="object {Current,Target,Master}", required=true)
    public DHTRingCurTargetZK.NodeType	nodeType;
    @Option(name="-v", usage="versionPair)", required=false)
    public String	versionPair;
}
