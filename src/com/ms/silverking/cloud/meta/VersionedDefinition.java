package com.ms.silverking.cloud.meta;


/**
 * Some definition that is versioned - typically in ZooKeeper.
 */
public interface VersionedDefinition {
    public static final long    NO_VERSION = Long.MIN_VALUE;
    public long getVersion();
}
