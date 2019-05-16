package com.ms.silverking.cloud.dht.daemon;

import com.ms.silverking.cloud.dht.daemon.storage.LRUReapPolicy;
import com.ms.silverking.cloud.dht.daemon.storage.NeverReapPolicy;
import com.ms.silverking.cloud.dht.daemon.storage.ReapOnIdlePolicy;
import com.ms.silverking.cloud.dht.daemon.storage.ReapPolicy;
import com.ms.silverking.text.ObjectDefParser2;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DHTNodeOptionsReflectionTest {
    @Test
    public void testReapPolicyReflection() {
        DHTNodeOptions options = new DHTNodeOptions();
        // All these ReapPolicies override its toString as "ObjectDefParser2.objectToString(this)"
        ReapPolicy p1 = new LRUReapPolicy();
        ReapPolicy p2 = new LRUReapPolicy(true, true, false, 1000, 2000, 3000);
        ReapPolicy p3 = new ReapOnIdlePolicy();
        ReapPolicy p4 = new ReapOnIdlePolicy(true, true, false, true, false, 1000, 500, 999, 777, 78, 182, ReapPolicy.EmptyTrashMode.BeforeAndAfterInitialReap);

        ReapPolicy[] policyTestSet1 = {p1, p2, p3, p4};
        for(ReapPolicy policy: policyTestSet1) {
            options.reapPolicy = ObjectDefParser2.toClassAndDefString(policy);
            ReapPolicy reflectedPolicy = options.getReapPolicy();

            assertEquals(policy.toString(), reflectedPolicy.toString());
        }

        // Only this policy does NOT override its toString
        ReapPolicy p5 = new NeverReapPolicy();
        options.reapPolicy = ObjectDefParser2.toClassAndDefString(p5);
        assertTrue(options.getReapPolicy() instanceof NeverReapPolicy);
    }
}
