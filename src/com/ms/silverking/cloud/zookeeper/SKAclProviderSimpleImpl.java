package com.ms.silverking.cloud.zookeeper;

import java.util.List;
import java.util.Map;

import org.apache.zookeeper.data.ACL;

public class SKAclProviderSimpleImpl implements SKAclProvider {
    private final List<ACL> defaultAcl;
    private final Map<String, List<ACL>> aclOverrides;

    public SKAclProviderSimpleImpl(List<ACL> defaultAcl, Map<String, List<ACL>> aclOverrides) {
        this.defaultAcl = defaultAcl;
        this.aclOverrides = aclOverrides;
    }

    public List<ACL> getDefaultAcl() { return defaultAcl; }
    public List<ACL> getAclForPath(String path) { return aclOverrides.getOrDefault(path, getDefaultAcl()); }
}