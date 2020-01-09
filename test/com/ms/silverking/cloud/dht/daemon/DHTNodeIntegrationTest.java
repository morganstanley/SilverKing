package com.ms.silverking.cloud.dht.daemon;

import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.util.DHTNodeTestUtil;
import com.ms.silverking.cloud.dht.util.TestDHTNode;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class DHTNodeIntegrationTest extends DHTNodeTestUtil{

    private int dhtPort = 10000 + new Random().nextInt(10000);
    private int replFactor = 1;

    //@Test
    public void testReadAndWrite() throws IOException, ClientException, KeeperException {
        String id = new UUIDBase(false).toString();
        String nodeName = "SK."+ id;
        String ringName = "ring."+ id;

        TestDHTNode dhtNode = getDhtNode(nodeName, dhtPort, ringName, replFactor);
        Log.warning("started dht in testWriteAndRead");
        try {
            SynchronousNamespacePerspective<String, String> syncNSP;
            ClientDHTConfiguration dhtConfig = dhtNode.getClientDHTConfiguration();

            syncNSP = new DHTClient().openSession(dhtConfig)
                    .openSyncNamespacePerspective("_MyNamespace" + id, String.class, String.class);
            syncNSP.put("Hello", "Hello world!");
            assert (syncNSP.get("Hello").equals("Hello world!"));
        } finally {
            dhtNode.stop();
            Log.warning("stopped dht in testWriteAndRead");
        }
    }

    //@Test
    public void testRecoveryOnSingleWrite() throws IOException, ClientException, KeeperException {
        String id = new UUIDBase(false).toString();
        String nodeName = "SK."+ id;
        String ringName = "ring."+ id;
        SynchronousNamespacePerspective<String, String> syncNSP;

        TestDHTNode dhtNode = getDhtNode(nodeName, dhtPort, ringName, replFactor);
        Log.warning("started dht in testRecoveryOnSingleWrite");
        ClientDHTConfiguration dhtConfig = dhtNode.getClientDHTConfiguration();;

        try {
            syncNSP = new DHTClient().openSession(dhtConfig)
                    .openSyncNamespacePerspective("_MyNamespace" + id, String.class, String.class);
            syncNSP.put("Hello", "Hello world!");
            assert (syncNSP.get("Hello").equals("Hello world!"));
        } finally {
            dhtNode.stop();
            Log.warning("stopped dht in testRecoveryOnSingleWrite");
        }

        TestDHTNode dhtNode2 = getDhtNode(nodeName, dhtNode.getDHTNodeConfiguration(), dhtConfig);
        Log.warning("restarted dht in testRecoveryOnSingleWrite");
        try {
            syncNSP = new DHTClient().openSession(dhtConfig)
                    .openSyncNamespacePerspective("_MyNamespace" + id, String.class, String.class);
            assert (syncNSP.get("Hello").equals("Hello world!"));
        } finally {
            dhtNode2.stop();
            Log.warning("stopped dht in testRecoveryOnSingleWrite again");
        }
    }

    @Test
    public void testRecoveryOnMutationFailure() throws IOException, ClientException, KeeperException {
        String id = new UUIDBase(false).toString();
        String nodeName = "SK."+ id;
        String ringName = "ring."+ id;
        SynchronousNamespacePerspective<String, String> syncNSP;

        TestDHTNode dhtNode = getDhtNode(nodeName, dhtPort, ringName, replFactor);
        Log.warning("started dht in testRecoveryOnMutationFailure");
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
            Log.warning("stopped dht in testRecoveryOnMutationFailure");
        }

        TestDHTNode dhtNode2 = getDhtNode(nodeName, dhtNode.getDHTNodeConfiguration(), dhtConfig);
        Log.warning("restarted dht in restRecoveryOnMutationFailure again");
        try {
            syncNSP = new DHTClient().openSession(dhtConfig)
                    .openSyncNamespacePerspective("_MyNamespace" + id, String.class, String.class);
            assert (syncNSP.get("Hello").equals("Hello world!"));
        } finally {
            dhtNode2.stop();
            Log.warning("stopped dht in testRecoveryOnMutationFailure again");
        }
    }
}
