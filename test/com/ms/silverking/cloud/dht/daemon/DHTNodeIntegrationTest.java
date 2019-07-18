package com.ms.silverking.cloud.dht.daemon;

import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.util.DHTNodeTestUtil;
import com.ms.silverking.cloud.dht.util.TestDHTNode;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import org.junit.Test;

public class DHTNodeIntegrationTest extends DHTNodeTestUtil{

    @Test
    public void testWriteAndRead() {
        try {
            String id = new UUIDBase(false).toString();
            TestDHTNode dhtNode = getDhtNode("SK."+ id, 1234, "ring."+ id, 1);
            Log.warning("started dht");
            SynchronousNamespacePerspective<String, String> syncNSP;
            ClientDHTConfiguration dhtConfig = dhtNode.getClientDHTConfiguration();

            syncNSP = new DHTClient().openSession(dhtConfig)
                    .openSyncNamespacePerspective("_MyNamespace"+id, String.class, String.class);
            syncNSP.put("Hello", "Hello world!");
            assert(syncNSP.get("Hello").equals("Hello world!"));
            dhtNode.stop();
            Log.warning("stopped dht");
        } catch (Exception e) {
            assert(false);
        }
    }

    @Test
    public void testRecoveryOnSingleWrite() {
        try {
            String id = new UUIDBase(false).toString();
            TestDHTNode dhtNode = getDhtNode("SK."+ id, 1234, "ring."+ id, 1);
            Log.warning("started dht");
            SynchronousNamespacePerspective<String, String> syncNSP;
            ClientDHTConfiguration dhtConfig = dhtNode.getClientDHTConfiguration();

            syncNSP = new DHTClient().openSession(dhtConfig)
                    .openSyncNamespacePerspective("_MyNamespace" + id, String.class, String.class);
            syncNSP.put("Hello", "Hello world!");
            assert(syncNSP.get("Hello").equals("Hello world!"));
            dhtNode.stop();
            Log.warning("stopped dht");

            TestDHTNode dhtNode2 = getDhtNode("SK."+ id, dhtNode.getDHTNodeConfiguration(), dhtConfig);
            Log.warning("started dht");
            syncNSP = new DHTClient().openSession(dhtConfig)
                    .openSyncNamespacePerspective("_MyNamespace" + id, String.class, String.class);
            assert(syncNSP.get("Hello").equals("Hello world!"));

            dhtNode2.stop();
            Log.warning("stopped dht");

        } catch (Exception e) {
            assert(false);
        }
    }

    @Test
    public void testRecoveryOnMutationFailure() {
        try {
            String id = new UUIDBase(false).toString();
            TestDHTNode dhtNode = getDhtNode("SK."+ id, 1234, "ring."+ id, 1);
            Log.warning("started dht");
            SynchronousNamespacePerspective<String, String> syncNSP;
            ClientDHTConfiguration dhtConfig = dhtNode.getClientDHTConfiguration();

            syncNSP = new DHTClient().openSession(dhtConfig)
                    .openSyncNamespacePerspective("_MyNamespace" + id, String.class, String.class);
            syncNSP.put("Hello", "Hello world!");
            assert(syncNSP.get("Hello").equals("Hello world!"));
            try {
                syncNSP.put("Hello", "Hello world2!");
            } catch(Exception e) {
                Log.logErrorWarning(e);
            }
            dhtNode.stop();

            TestDHTNode dhtNode2 = getDhtNode("SK."+ id, dhtNode.getDHTNodeConfiguration(), dhtConfig);
            Log.warning("started dht");
            syncNSP = new DHTClient().openSession(dhtConfig)
                    .openSyncNamespacePerspective("_MyNamespace" + id, String.class, String.class);
            assert(syncNSP.get("Hello").equals("Hello world!"));

            dhtNode2.stop();
            Log.warning("stopped dht");

        } catch (Exception e) {
            assert(false);
        }
    }
}
