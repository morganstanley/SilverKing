package com.ms.silverking.cloud.meta;

import java.util.Map;

public interface ChildrenListener {
    public void childrenChanged(String basePath, Map<String,byte[]> childStates);
}
