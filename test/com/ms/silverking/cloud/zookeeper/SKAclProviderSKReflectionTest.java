package com.ms.silverking.cloud.zookeeper;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.zookeeper.data.ACL;
import org.junit.Test;

public class SKAclProviderSKReflectionTest {
    @Test
    public void testSKStringDefReflection() {
        DummySKAclProviderImpl aclProvider = new DummySKAclProviderImpl();
        String def = aclProvider.toSKDef();
        SKAclProvider acl = SKAclProvider.parse(def);

        String nonOverridePath = DummySKAclProviderImpl.overridePath + "/not/in/the/map";
        
        Object[][] testCases = {
    		{aclProvider.getDefaultAcl(),       aclProvider.getAclForPath(nonOverridePath), aclProvider.getAclForPath(DummySKAclProviderImpl.overridePath)},
    		{DummySKAclProviderImpl.defaultAcl, DummySKAclProviderImpl.defaultAcl,          DummySKAclProviderImpl.overrideAcl},
        };
        
        for (Object[] testCase : testCases) {
        	List<ACL> expectedDefaultAcl     = (List<ACL>)testCase[0];
        	List<ACL> expectedNonOverrideAcl = (List<ACL>)testCase[1];
        	List<ACL> expectedOverrideAcl    = (List<ACL>)testCase[2];
        	
            checkAcl(expectedDefaultAcl,     acl.getDefaultAcl());
            checkAcl(expectedNonOverrideAcl, acl.getAclForPath(nonOverridePath));
            checkAcl(expectedOverrideAcl,    acl.getAclForPath(DummySKAclProviderImpl.overridePath));
        }
    }
    
    private void checkAcl(List<ACL> expected, List<ACL> actual) {
        assertEquals(expected, actual);
    }
}
