package com.ms.silverking.cloud.dht.daemon;

import org.kohsuke.args4j.Option;

import com.ms.silverking.cloud.dht.daemon.storage.ReapMode;

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
    
	@Option(name="-reapMode", usage="reapMode", required=false)
	ReapMode reapMode = ReapMode.OnStartup;	
	
	// temp legacy -r support
	// remove once -r usage removed
	public ReapMode getReapMode() {
		if (disableReap) {
			return ReapMode.None;
		} else {
			return reapMode;
		}
	}	
    
    @Option(name="-leaveTrash", usage="leaveTrash", required=false)
    public boolean leaveTrash = false;
}
