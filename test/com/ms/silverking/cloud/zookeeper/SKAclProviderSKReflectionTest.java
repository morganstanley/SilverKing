package com.ms.silverking.cloud.zookeeper;

import com.ms.silverking.text.ObjectDefParser2;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SKAclProviderSKReflectionTest {
    @Test
    public void testSKStringDefReflection() {
        DummySKAclProviderImpl imp = new DummySKAclProviderImpl();
        String def = imp.toSKDef();
        SKAclProvider acl = SKAclProvider.parse(def);

        String nonOverridePath = DummySKAclProviderImpl.overridePath + "/not/in/the/map";
        assertEquals(acl.getDefaultAcl(), imp.getDefaultAcl());
        assertEquals(acl.getAclForPath(nonOverridePath), imp.getAclForPath(nonOverridePath));
        assertEquals(acl.getAclForPath(DummySKAclProviderImpl.overridePath), imp.getAclForPath(DummySKAclProviderImpl.overridePath));

        assertEquals(acl.getDefaultAcl(), DummySKAclProviderImpl.defaultAcl);
        assertEquals(acl.getAclForPath(nonOverridePath), DummySKAclProviderImpl.defaultAcl);
        assertEquals(acl.getAclForPath(DummySKAclProviderImpl.overridePath), DummySKAclProviderImpl.overrideAcl);
    }
}
