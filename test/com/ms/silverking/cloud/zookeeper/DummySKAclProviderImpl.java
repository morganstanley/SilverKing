package com.ms.silverking.cloud.zookeeper;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ms.silverking.text.ObjectDefParser2;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

public class DummySKAclProviderImpl implements SKAclProvider {
    static final List<ACL> defaultAcl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    static final String overridePath = "/dummy/node";
    static final List<ACL> overrideAcl = ZooDefs.Ids.CREATOR_ALL_ACL;
    private static final Map<String, List<ACL>> aclOverrides = new HashMap<>();

    static {
        aclOverrides.put(overridePath, overrideAcl);
        ObjectDefParser2.addParser(new DummySKAclProviderImpl());
    }

    public String toSKDef() {
        // TODO: update this later when we got GitHub access and refactor ObjectDefParser2 in future
        return "<" + this.getClass().getCanonicalName() + ">{" + ObjectDefParser2.objectToString(this) + "}";
    }

    public DummySKAclProviderImpl() {

    }

    public List<ACL> getDefaultAcl() {
        return defaultAcl;
    }

    public List<ACL> getAclForPath(String path) {
        return aclOverrides.getOrDefault(path, getDefaultAcl());
    }
}