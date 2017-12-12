package com.ms.silverking.cloud.dht.daemon;

import org.kohsuke.args4j.Option;

public class DHTNodeOptions {
    public static final int	defaultInactiveNodeTimeoutSeconds = Integer.MAX_VALUE;
    
    @Option(name="-n", usage="dhtName", required=true)
    public String dhtName;
    @Option(name="-z", usage="zkConfig", required=true)
    public String zkConfig;
    @Option(name="-into", usage="inactiveNodeTimeoutSeconds", required=false)
    public int inactiveNodeTimeoutSeconds = defaultInactiveNodeTimeoutSeconds;
    @Option(name="-r", usage="disableReap", required=false)
    public boolean disableReap = false;
    @Option(name="-leaveTrash", usage="leaveTrash", required=false)
    public boolean leaveTrash = false;
}
