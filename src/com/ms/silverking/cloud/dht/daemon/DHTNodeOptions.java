package com.ms.silverking.cloud.dht.daemon;

import org.kohsuke.args4j.Option;

import com.ms.silverking.cloud.dht.daemon.storage.NeverReapPolicy;
import com.ms.silverking.cloud.dht.daemon.storage.ReapOnIdlePolicy;
import com.ms.silverking.cloud.dht.daemon.storage.ReapPolicy;
import com.ms.silverking.text.ObjectDefParser2;

public class DHTNodeOptions {
    public static final int	defaultInactiveNodeTimeoutSeconds = Integer.MAX_VALUE;
    
    @Option(name="-n", usage="dhtName", required=true)
    public String dhtName;
    
    @Option(name="-z", usage="zkConfig", required=true)
    public String zkConfig;
    
    @Option(name="-into", usage="inactiveNodeTimeoutSeconds", required=false)
    public int inactiveNodeTimeoutSeconds = defaultInactiveNodeTimeoutSeconds;
    
	@Option(name="-reapPolicy", usage="reapPolicy", required=false)
	String reapPolicy = new ReapOnIdlePolicy().toString();
	
	public ReapPolicy getReapPolicy() {
		if (reapPolicy.contains("idleReapPauseMillis")) { // FUTURE - remove workaround when parser support available
			return (ReapPolicy)ObjectDefParser2.parse(ReapOnIdlePolicy.class, reapPolicy);
		} else {
			return NeverReapPolicy.instance;
		}
	}
}
