package com.ms.silverking.cloud.dht.client;

/**
 * Provides a constant version
 */
public class ConstantVersionProvider implements VersionProvider {
    private final long  version;
    
    public ConstantVersionProvider(long version) {
        this.version = version;
    }

    @Override
    public long getVersion() {
        return version;
    }
}
