package com.ms.silverking.cloud.toporing.meta;

import com.ms.silverking.collection.Pair;

public interface RingChangeListener {
    public void ringChanged(String ringName, String basePath, Pair<Long,Long> version, long creationTimeMillis);
}
