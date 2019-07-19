package com.ms.silverking.cloud.dht.daemon;

import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.util.DHTNodeTestUtil;
import com.ms.silverking.cloud.dht.util.TestDHTNode;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

public class DHTNodeIntegrationTest extends DHTNodeTestUtil{

    private int dhtPort = 10000 + new Random().nextInt(10000);
    private int replFactor = 1;

    @Test
    public void testWriteAndRead() throws IOException, ClientException {
        String id = new UUIDBase(false).toString();
        String nodeName = "SK."+ id;
        String ringName = "ring."+ id;

        TestDHTNode dhtNode = getDhtNode(nodeName, dhtPort, ringName, replFactor);
        Log.warning("started dht");
        try {
            SynchronousNamespacePerspective<String, String> syncNSP;
            ClientDHTConfiguration dhtConfig = dhtNode.getClientDHTConfiguration();

            syncNSP = new DHTClient().openSession(dhtConfig)
                    .openSyncNamespacePerspective("_MyNamespace" + id, String.class, String.class);
            syncNSP.put("Hello", "Hello world!");
            assert (syncNSP.get("Hello").equals("Hello world!"));
        } finally {
            dhtNode.stop();
            Log.warning("stopped dht");
        }

    }

    @Test
    public void testRecoveryOnSingleWrite() throws IOException, ClientException {
        String id = new UUIDBase(false).toString();
        String nodeName = "SK."+ id;
        String ringName = "ring."+ id;
        SynchronousNamespacePerspective<String, String> syncNSP;

        TestDHTNode dhtNode = getDhtNode(nodeName, dhtPort, ringName, replFactor);
        Log.warning("started dht");
        ClientDHTConfiguration dhtConfig = dhtNode.getClientDHTConfiguration();;

        try {
            syncNSP = new DHTClient().openSession(dhtConfig)
                    .openSyncNamespacePerspective("_MyNamespace" + id, String.class, String.class);
            syncNSP.put("Hello", "Hello world!");
            assert (syncNSP.get("Hello").equals("Hello world!"));
        } finally {
            dhtNode.stop();
            Log.warning("stopped dht");
        }

        TestDHTNode dhtNode2 = getDhtNode(nodeName, dhtPort, ringName, replFactor);
        Log.warning("started dht");
        try {
            syncNSP = new DHTClient().openSession(dhtConfig)
                    .openSyncNamespacePerspective("_MyNamespace" + id, String.class, String.class);
            assert (syncNSP.get("Hello").equals("Hello world!"));
        } finally {
            dhtNode2.stop();
            Log.warning("stopped dht");
        }
    }

    @Test
    public void testRecoveryOnMutationFailure() throws IOException, ClientException {
        String id = new UUIDBase(false).toString();
        String nodeName = "SK."+ id;
        String ringName = "ring."+ id;
        SynchronousNamespacePerspective<String, String> syncNSP;

        TestDHTNode dhtNode = getDhtNode(nodeName, dhtPort, ringName, replFactor);
        Log.warning("started dht");
        ClientDHTConfiguration dhtConfig = dhtNode.getClientDHTConfiguration();

        try {
            syncNSP = new DHTClient().openSession(dhtConfig)
                .openSyncNamespacePerspective("_MyNamespace" + id, String.class, String.class);
            syncNSP.put("Hello", "Hello world!");
            assert(syncNSP.get("Hello").equals("Hello world!"));
            syncNSP.put("Hello", "Hello world2!");
        } catch(Exception e) {
            Log.logErrorWarning(e);
        } finally {
            dhtNode.stop();
        }

        TestDHTNode dhtNode2 = getDhtNode(nodeName, dhtPort, ringName, replFactor);
        Log.warning("started dht");
        try {
            syncNSP = new DHTClient().openSession(dhtConfig)
                    .openSyncNamespacePerspective("_MyNamespace" + id, String.class, String.class);
            assert (syncNSP.get("Hello").equals("Hello world!"));
        } finally {
            dhtNode2.stop();
            Log.warning("stopped dht");
        }
    }
}
