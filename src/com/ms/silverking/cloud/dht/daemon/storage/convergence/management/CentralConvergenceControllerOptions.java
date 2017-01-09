package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import org.kohsuke.args4j.Option;

public class CentralConvergenceControllerOptions {
    @Option(name="-g", usage="GridConfig", required=true)
    public String gridConfig;
}
