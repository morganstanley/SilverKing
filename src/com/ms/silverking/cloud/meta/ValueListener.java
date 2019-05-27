package com.ms.silverking.cloud.meta;

import org.apache.zookeeper.data.Stat;

public interface ValueListener {
    public void newValue(String basePath, byte[] value, Stat stat);
}
