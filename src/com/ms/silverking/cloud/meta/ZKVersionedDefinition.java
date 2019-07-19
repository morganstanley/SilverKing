package com.ms.silverking.cloud.meta;


/**
 * Some definition that is versioned - typically in ZooKeeper.
 */
public interface ZKVersionedDefinition extends VersionedDefinition {
    public long getMzxid();
}
