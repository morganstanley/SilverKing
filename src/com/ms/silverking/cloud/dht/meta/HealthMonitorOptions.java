package com.ms.silverking.cloud.dht.meta;

import org.kohsuke.args4j.Option;

public class HealthMonitorOptions {
	static final int	NO_DOCTOR = 0;
	
    @Option(name="-g", usage="GridConfig", required=true)
    public String gridConfig;
    @Option(name="-c", usage="convictionLimits", required=true)
    public String convictionLimits;
    @Option(name="-i", usage="intervalSeconds", required=false)
    public int  watchIntervalSeconds = 15;
    @Option(name="-G", usage="guiltThreshold", required=false)
    public int  guiltThreshold = 3;
    @Option(name="-dri", usage="doctorRoundIntervalSeconds", required=false)
    public int  doctorRoundIntervalSeconds = NO_DOCTOR;
	@Option(name="-forceUnsafe", usage="forceInclusionOfUnsafeExcludedServers", required=false)
	boolean	forceInclusionOfUnsafeExcludedServers = false;
    
    public boolean doctorRequested() {
    	return doctorRoundIntervalSeconds != NO_DOCTOR;
    }
}
