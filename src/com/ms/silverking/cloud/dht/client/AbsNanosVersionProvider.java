package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.time.AbsNanosTimeSource;

/**
 * Provides versions from an AbsMillisTimeSource.
 */
public class AbsNanosVersionProvider implements VersionProvider {
    private final AbsNanosTimeSource   absNanosTimeSource;
    
    public AbsNanosVersionProvider(AbsNanosTimeSource absNanosTimeSource) {
        this.absNanosTimeSource = absNanosTimeSource;
    }
    
    @Override
    public long getVersion() {
        return absNanosTimeSource.absTimeNanos();
    }
}
